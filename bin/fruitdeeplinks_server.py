#!/usr/bin/env python3
"""
fruitdeeplinks_server.py - Web server for FruitDeepLinks
Features: Admin panel, live logging, stream proxying, filtering (future),
auto-refresh with APScheduler
"""

import os
import sys
import json
import sqlite3
import subprocess
import threading
import time
import tempfile
import requests
from pathlib import Path
from datetime import datetime
from collections import deque

from flask import (
    Flask,
    jsonify,
    request,
    send_file,
    Response,
    stream_with_context,
)
from flask_cors import CORS
import urllib.parse

# APScheduler (for auto-refresh)
try:
    from apscheduler.schedulers.background import BackgroundScheduler

    APSCHEDULER_AVAILABLE = True
except ImportError:
    BackgroundScheduler = None
    APSCHEDULER_AVAILABLE = False

# Import provider utilities
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from provider_utils import get_provider_display_name, get_all_providers_from_db
except ImportError:
    # Fallback if provider_utils not available
    def get_provider_display_name(provider):
        return provider.upper()

    def get_all_providers_from_db(conn):
        return []


# Import logical service mapper
try:
    from logical_service_mapper import (
        get_all_logical_services_with_counts,
        get_service_display_name as get_logical_service_display_name,
    )

    LOGICAL_SERVICES_AVAILABLE = True
except ImportError:
    LOGICAL_SERVICES_AVAILABLE = False
    print("Warning: logical_service_mapper not available, using basic provider grouping")

# Configuration
DB_PATH = Path(os.getenv("FRUIT_DB_PATH") or os.getenv("PEACOCK_DB_PATH") or "/app/data/fruit_events.db")
OUT_DIR = Path(os.getenv("OUT_DIR", "/app/out"))
BIN_DIR = Path(os.getenv("BIN_DIR", "/app/bin"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/app/logs"))

# CDVR Detector Configuration
CDVR_SERVER_IP = os.getenv("CHANNELS_DVR_IP", "192.168.86.72")
CDVR_SERVER_PORT = int(os.getenv("CDVR_SERVER_PORT", "8089"))
CDVR_API_PORT = int(os.getenv("CDVR_API_PORT", "57000"))
# CDVR_DVR_PATH should be set by user - empty means detector disabled
CDVR_DVR_PATH = os.getenv("CDVR_DVR_PATH", "")
NUM_LANES = int(os.getenv("FRUIT_LANES", "50"))

# Detector globals
DUMMY_SEGMENT_PATH = None
# Use /mnt/dvr mount point (mapped from user's CDVR_DVR_PATH via docker-compose)
DETECTOR_ENABLED = bool(CDVR_DVR_PATH and CDVR_DVR_PATH.strip())
STREAMLINK_DIR = Path("/mnt/dvr") / "Imports" / "Videos" / "FruitDeepLinks" if DETECTOR_ENABLED else None


# Detector debounce (avoid spawning multiple detector threads per lane)
DETECT_DEBOUNCE_SECONDS = float(os.getenv('DETECT_DEBOUNCE_SECONDS', '3'))
DETECT_LAST_SPAWN = {}  # lane_number -> last_spawn_epoch
DETECT_LAST_SPAWN_LOCK = threading.Lock()

# Create Flask app
app = Flask(__name__)
CORS(app)

# Global state
log_buffer = deque(maxlen=1000)  # Keep last 1000 log lines
refresh_status = {
    "running": False,
    "last_run": None,
    "last_status": None,
    "current_step": None,
    "last_run_manual": None,
    "last_status_manual": None,
    "last_run_auto": None,
    "last_status_auto": None,
}

# APScheduler globals
scheduler = None
auto_refresh_job = None
auto_refresh_settings = {
    "enabled": False,
    "time": "02:30",  # HH:MM local time
}


# ==================== Logging ====================
class LogCapture:
    """Captures logs and stores them in memory"""

    def __init__(self):
        self.enabled = True

    def write(self, message):
        if self.enabled and message.strip():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_buffer.append(f"[{timestamp}] {message.strip()}")

    def flush(self):
        pass


log_capture = LogCapture()


def log(message, level="INFO"):
    """Add a log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] {message}"
    log_buffer.append(log_line)
    print(log_line)


# ==================== Database / Pref Utilities ====================
def get_db_connection():
    """Get database connection"""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH))


def _load_raw_preferences():
    """
    Load raw key/value prefs from user_preferences table (no JSON decoding).
    Used for auto-refresh settings so they can coexist with filters.
    """
    prefs = {}
    conn = get_db_connection()
    if not conn:
        return prefs

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            conn.close()
            return prefs

        cur.execute("SELECT key, value FROM user_preferences")
        for key, value in cur.fetchall():
            prefs[key] = value
        conn.close()
    except Exception as e:
        log(f"Error loading raw preferences: {e}", "ERROR")
    return prefs


def get_user_preferences():
    """Get user filtering preferences"""
    conn = get_db_connection()
    if not conn:
        return {
            "enabled_services": [],
            "disabled_sports": [],
            "disabled_leagues": [],
            "service_priorities": {},
            "amazon_penalty": True
        }

    try:
        cur = conn.cursor()
        # Check if user_preferences table exists
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            return {
                "enabled_services": [],
                "disabled_sports": [],
                "disabled_leagues": [],
                "service_priorities": {},
                "amazon_penalty": True
            }

        prefs = {}
        cur.execute("SELECT key, value FROM user_preferences")
        for row in cur.fetchall():
            key, value = row
            try:
                prefs[key] = json.loads(value) if value else []
            except Exception:
                prefs[key] = []

        conn.close()
        
        # Parse and return all preferences including priorities
        result = {
            "enabled_services": prefs.get("enabled_services", []),
            "disabled_sports": prefs.get("disabled_sports", []),
            "disabled_leagues": prefs.get("disabled_leagues", []),
        }
        
        # Add service_priorities (merge with defaults if needed)
        if "service_priorities" in prefs:
            try:
                custom = json.loads(prefs["service_priorities"]) if isinstance(prefs["service_priorities"], str) else prefs["service_priorities"]
                result["service_priorities"] = custom if custom else {}
            except Exception:
                result["service_priorities"] = {}
        else:
            result["service_priorities"] = {}
        
        # Add amazon_penalty
        if "amazon_penalty" in prefs:
            try:
                result["amazon_penalty"] = bool(json.loads(prefs["amazon_penalty"]) if isinstance(prefs["amazon_penalty"], str) else prefs["amazon_penalty"])
            except Exception:
                result["amazon_penalty"] = True
        else:
            result["amazon_penalty"] = True
        
        return result
    except Exception as e:
        log(f"Error loading preferences: {e}", "ERROR")
        return {
            "enabled_services": [],
            "disabled_sports": [],
            "disabled_leagues": [],
            "service_priorities": {},
            "amazon_penalty": True
        }


def save_user_preferences(prefs):
    """Save user filtering preferences"""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        now = datetime.utcnow().isoformat()

        # Make sure table exists (in case this is first write)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_utc TEXT
            )
        """
        )

        for key, value in prefs.items():
            cur.execute(
                "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
                (key, json.dumps(value), now),
            )

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log(f"Error saving preferences: {e}", "ERROR")
        return False


# -------- Auto-refresh settings (stored in user_preferences) --------
def get_auto_refresh_settings():
    """Get auto-refresh settings from DB or defaults"""
    # Defaults, with optional env overrides
    settings = {
        "enabled": os.getenv("AUTO_REFRESH_ENABLED", "1").lower()
        not in ("0", "false", "no"),
        "time": os.getenv("AUTO_REFRESH_TIME", "02:30"),
    }

    prefs = _load_raw_preferences()
    if not prefs:
        return settings

    if "auto_refresh_enabled" in prefs:
        try:
            settings["enabled"] = bool(json.loads(prefs["auto_refresh_enabled"]))
        except Exception:
            pass

    if "auto_refresh_time" in prefs:
        try:
            settings["time"] = json.loads(prefs["auto_refresh_time"])
        except Exception:
            settings["time"] = prefs["auto_refresh_time"]

    return settings


def save_auto_refresh_settings(settings):
    """Persist auto-refresh settings to DB"""
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_utc TEXT
            )
        """
        )
        now = datetime.utcnow().isoformat()

        cur.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
            ("auto_refresh_enabled", json.dumps(bool(settings.get("enabled", False))), now),
        )
        cur.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
            ("auto_refresh_time", json.dumps(settings.get("time", "02:30")), now),
        )

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log(f"Error saving auto-refresh settings: {e}", "ERROR")
        return False


def get_available_filters():
    """Get available sports, leagues, and providers for filtering"""
    conn = get_db_connection()
    if not conn:
        return {"providers": [], "sports": [], "leagues": []}

    try:
        cur = conn.cursor()

        # Get providers using logical service mapping
        providers = []
        try:
            if LOGICAL_SERVICES_AVAILABLE:
                # Use logical service mapper to get web services broken down
                service_counts = get_all_logical_services_with_counts(conn)

                for service_code, count in sorted(
                    service_counts.items(), key=lambda x: -x[1]
                ):
                    display_name = get_logical_service_display_name(service_code)
                    providers.append(
                        {
                            "scheme": service_code,
                            "name": display_name,
                            "count": count,
                        }
                    )
            else:
                # Fallback: use raw provider grouping
                cur.execute(
                    """
                    SELECT provider, COUNT(*) as count
                    FROM playables
                    WHERE provider IS NOT NULL AND provider != ''
                    GROUP BY provider
                    ORDER BY count DESC
                """
                )
                for row in cur.fetchall():
                    provider, count = row
                    display_name = get_provider_display_name(provider)
                    providers.append(
                        {
                            "scheme": provider,
                            "name": display_name,
                            "count": count,
                        }
                    )
        except Exception as e:
            log(f"Error loading providers: {e}", "ERROR")

        # Get sports from genres_json - simpler approach
        sports = {}
        cur.execute(
            """
            SELECT genres_json, COUNT(*) as event_count
            FROM events 
            WHERE end_utc > datetime('now')
            AND genres_json IS NOT NULL 
            AND genres_json != '[]'
            GROUP BY genres_json
        """
        )
        for row in cur.fetchall():
            genres_json, event_count = row
            try:
                genres = json.loads(genres_json)
                for genre in genres:
                    if genre and isinstance(genre, str):
                        sports[genre] = sports.get(genre, 0) + event_count
            except Exception:
                pass

        sports_list = [
            {"name": k, "count": v}
            for k, v in sorted(sports.items(), key=lambda x: -x[1])
        ]

        # Get leagues from classification_json
        leagues = {}
        cur.execute(
            """
            SELECT classification_json, COUNT(*) as event_count
            FROM events
            WHERE end_utc > datetime('now')
            AND classification_json IS NOT NULL
            AND classification_json != '[]'
            GROUP BY classification_json
        """
        )
        for row in cur.fetchall():
            class_json, event_count = row
            try:
                classifications = json.loads(class_json)
                for item in classifications:
                    if isinstance(item, dict) and item.get("type") == "league":
                        league_name = item.get("value")
                        if league_name:
                            leagues[league_name] = leagues.get(league_name, 0) + event_count
            except Exception:
                pass

        leagues_list = [
            {"name": k, "count": v}
            for k, v in sorted(leagues.items(), key=lambda x: -x[1])[:50]
        ]

        conn.close()
        return {
            "providers": providers,
            "sports": sports_list,
            "leagues": leagues_list,
        }
    except Exception as e:
        log(f"Error getting filters: {e}", "ERROR")
        return {"providers": [], "sports": [], "leagues": []}


def get_db_stats():
    """Get database statistics + file timestamp/size"""
    if not DB_PATH.exists():
        return {"error": "Database not found"}

    try:
        stats = {}

        # File-level info
        stat = DB_PATH.stat()
        stats["db_path"] = str(DB_PATH)
        stats["db_size"] = stat.st_size
        stats["db_modified"] = datetime.fromtimestamp(stat.st_mtime).isoformat()

        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()

        # Total events
        cur.execute("SELECT COUNT(*) FROM events")
        stats["total_events"] = cur.fetchone()[0]

        # Future events
        cur.execute("SELECT COUNT(*) FROM events WHERE end_utc > datetime('now')")
        stats["future_events"] = cur.fetchone()[0]

        # Events by provider (top 10)
        cur.execute(
            """
            SELECT channel_name, COUNT(*) as count 
            FROM events 
            WHERE end_utc > datetime('now')
            GROUP BY channel_name 
            ORDER BY count DESC 
            LIMIT 10
        """
        )
        stats["top_providers"] = [
            {"name": row[0], "count": row[1]} for row in cur.fetchall()
        ]

        # Lane statistics (if tables exist)
        try:
            cur.execute("SELECT COUNT(*) FROM lanes")
            stats["lane_count"] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM lane_events WHERE is_placeholder = 0")
            stats["scheduled_events"] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM lane_events WHERE is_placeholder = 1")
            stats["placeholders"] = cur.fetchone()[0]
        except Exception:
            stats["lane_count"] = 0
            stats["scheduled_events"] = 0
            stats["placeholders"] = 0

        conn.close()
        return stats

    except Exception as e:
        return {"error": str(e)}


# Import filter integration helpers (shared with CLI exporters)
try:
    from filter_integration import (
        load_user_preferences,
        should_include_event,
        get_best_deeplink_for_event,
        get_fallback_deeplink,
    )
    FILTERING_AVAILABLE = True
except ImportError:
    FILTERING_AVAILABLE = False

    def load_user_preferences(conn):
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}

    def should_include_event(event, prefs):
        return True

    def get_best_deeplink_for_event(conn, event_id, enabled_services):
        return None

    def get_fallback_deeplink(event):
        return None


def get_event_link_columns(conn):
    """Inspect the events table and determine UID and deeplink columns.

    Returns (uid_col, primary_deeplink_col, full_deeplink_col).
    Some columns may be None if not present.
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(events)")
        rows = cur.fetchall()
    except Exception:
        return "id", None, None

    column_names = {row[1] for row in rows}

    # Event UID column
    for candidate in ("event_uid", "event_id", "uid", "pvid"):
        if candidate in column_names:
            uid_col = candidate
            break
    else:
        uid_col = "id"  # Fallback to primary key

    # Primary deeplink column
    primary_deeplink_col = None
    if "deeplink_url" in column_names:
        primary_deeplink_col = "deeplink_url"
    elif "deeplink" in column_names:
        primary_deeplink_col = "deeplink"
    elif "deeplink_url_full" in column_names:
        primary_deeplink_col = "deeplink_url_full"

    # Full deeplink column (if present)
    full_deeplink_col = "deeplink_url_full" if "deeplink_url_full" in column_names else None

    return uid_col, primary_deeplink_col, full_deeplink_col


def get_event_link_info(conn, event_id, uid_col, primary_deeplink_col, full_deeplink_col):
    """Fetch UID + deeplink info for a given event_id.

    Returns dict with:
      {
        "event_uid": str | None,
        "deeplink_url": str | None,
        "deeplink_url_full": str | None,
      }

    Resolution priority (mirrors direct.m3u exporter):
      1. Explicit deeplink columns in `events` table, if present and non-null.
      2. `filter_integration.get_best_deeplink_for_event` (respects enabled services).
      3. `filter_integration.get_fallback_deeplink` using raw_attributes_json.
      4. Peacock web deeplink (for non-Apple events with pvid).
      5. Apple TV fallback using `playables.playable_url`.
      6. Final fallback: `apple_tv_url` from events.raw_attributes_json.
    """
    cur = conn.cursor()

    # Inspect schema for columns we care about
    try:
        cur.execute("PRAGMA table_info(events)")
        schema_rows = cur.fetchall()
        column_names = {row[1] for row in schema_rows}
    except Exception:
        column_names = set()

    columns = []
    # UID column (if present)
    if uid_col and uid_col in column_names:
        columns.append(uid_col)
    # Primary key id
    if "id" in column_names and "id" not in columns:
        columns.append("id")
    # Optional helpers
    if "pvid" in column_names:
        columns.append("pvid")
    if "channel_name" in column_names:
        columns.append("channel_name")
    if "raw_attributes_json" in column_names:
        columns.append("raw_attributes_json")
    # Explicit deeplink columns, if they exist
    if primary_deeplink_col and primary_deeplink_col in column_names and primary_deeplink_col not in columns:
        columns.append(primary_deeplink_col)
    if full_deeplink_col and full_deeplink_col in column_names and full_deeplink_col not in columns:
        columns.append(full_deeplink_col)

    if not columns:
        return {"event_uid": None, "deeplink_url": None, "deeplink_url_full": None}

    col_expr = ", ".join(columns)
    try:
        cur.execute(f"SELECT {col_expr} FROM events WHERE id = ?", (event_id,))
        row = cur.fetchone()
    except Exception:
        return {"event_uid": None, "deeplink_url": None, "deeplink_url_full": None}

    if not row:
        return {"event_uid": None, "deeplink_url": None, "deeplink_url_full": None}

    data = dict(zip(columns, row))

    event_uid = data.get(uid_col) if uid_col in data else None
    pvid = data.get("pvid")
    channel_name = data.get("channel_name")
    raw_json = data.get("raw_attributes_json")

    # explicit deeplink columns (if any)
    primary_value = data.get(primary_deeplink_col) if primary_deeplink_col else None
    full_value = data.get(full_deeplink_col) if full_deeplink_col else None

    # Build a minimal event dict for filter_integration fallback
    event_row = {
        "id": event_id,
        "pvid": pvid,
        "channel_name": channel_name,
        "raw_attributes_json": raw_json,
    }

    # Start with explicit columns
    deeplink_url = primary_value or full_value

    # 2. filter_integration best deeplink / fallback
    if not deeplink_url and FILTERING_AVAILABLE:
        try:
            prefs = load_user_preferences(conn)
        except Exception:
            prefs = {"enabled_services": []}
        enabled_services = prefs.get("enabled_services", [])

        # Try best deeplink for event
        try:
            candidate = get_best_deeplink_for_event(conn, event_id, enabled_services)
        except Exception:
            candidate = None
        if candidate:
            deeplink_url = candidate
        else:
            # fallback based on raw_attributes_json
            try:
                fallback = get_fallback_deeplink(event_row)
            except Exception:
                fallback = None
            if fallback:
                deeplink_url = fallback

    # 3. Peacock web fallback (for non-Apple events with pvid)
    if not deeplink_url and pvid and not str(event_id).startswith("appletv-"):
        try:
            payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
            deeplink_url = "https://www.peacocktv.com/deeplink?deeplinkData=" + urllib.parse.quote(
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False), safe=""
            )
        except Exception:
            pass

    # 4. Apple TV: playable_url from playables table
    if not deeplink_url:
        try:
            cur.execute(
                """
                SELECT playable_url
                FROM playables
                WHERE event_id = ? AND playable_url IS NOT NULL
                ORDER BY priority ASC
                LIMIT 1
                """,
                (event_id,),
            )
            prow = cur.fetchone()
            if prow and prow[0]:
                deeplink_url = prow[0]
        except Exception:
            pass

    # 5. Final fallback: apple_tv_url from raw_attributes_json (if present)
    apple_url = None
    if raw_json:
        try:
            raw = json.loads(raw_json)
            apple_url = raw.get("apple_tv_url")
        except Exception:
            apple_url = None

    if not deeplink_url and apple_url:
        deeplink_url = apple_url

    # deeplink_url_full: prefer explicit full_value, then deeplink_url, then apple_url
    deeplink_full = full_value or deeplink_url or apple_url

    return {
        "event_uid": event_uid,
        "deeplink_url": deeplink_url,
        "deeplink_url_full": deeplink_full,
    }



def get_playable_id_for_event(conn, event_id: str, provider_code: str = None) -> str:
    """Best-effort lookup of playables.playable_id for an event (+ optional provider).

    This is mainly used to convert scheme deeplinks (e.g. sportscenter://...playChannel=espn1)
    into a working ESPN Watch HTTP URL which requires the playable_id (airing/playback id).
    """
    if not event_id:
        return None

    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(playables)")
        cols = {row[1] for row in cur.fetchall()}

        if "playable_id" not in cols:
            return None

        # Prefer a provider match when possible (logical_service is preferred when present)
        if provider_code:
            if "logical_service" in cols:
                cur.execute(
                    """
                    SELECT playable_id
                    FROM playables
                    WHERE event_id = ?
                      AND playable_id IS NOT NULL
                      AND playable_id != ''
                      AND (logical_service = ? OR provider = ?)
                    ORDER BY priority ASC
                    LIMIT 1
                    """,
                    (event_id, provider_code, provider_code),
                )
            else:
                cur.execute(
                    """
                    SELECT playable_id
                    FROM playables
                    WHERE event_id = ?
                      AND playable_id IS NOT NULL
                      AND playable_id != ''
                      AND provider = ?
                    ORDER BY priority ASC
                    LIMIT 1
                    """,
                    (event_id, provider_code),
                )
        else:
            cur.execute(
                """
                SELECT playable_id
                FROM playables
                WHERE event_id = ?
                  AND playable_id IS NOT NULL
                  AND playable_id != ''
                ORDER BY priority ASC
                LIMIT 1
                """,
                (event_id,),
            )

        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None

def get_current_events_by_lane(conn, at_ts=None):
    """Return dict of lane_id -> minimal current event row at the given time.

    Uses lane_events + events to find the event where:
      datetime(start_utc) <= datetime(at_ts) < datetime(end_utc)
    """
    from datetime import datetime as _dt

    if at_ts is None:
        at_ts = _dt.utcnow().isoformat(timespec="seconds")

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                le.lane_id,
                le.event_id,
                le.start_utc,
                le.end_utc,
                le.is_placeholder,
                e.title,
                e.channel_name,
                e.synopsis
            FROM lane_events le
            LEFT JOIN events e ON le.event_id = e.id
            WHERE datetime(le.start_utc) <= datetime(?)
              AND datetime(le.end_utc) > datetime(?)
            ORDER BY le.lane_id, le.start_utc
            """,
            (at_ts, at_ts),
        )
    except Exception:
        return {}

    current_by_lane = {}
    for row in cur.fetchall():
        lane_id = row["lane_id"]
        if lane_id not in current_by_lane:
            current_by_lane[lane_id] = dict(row)
    return current_by_lane


def get_fallback_event_for_lane(conn, lane_id, at_ts):
    """Get the most recent non-placeholder event for a lane within padding window.
    
    This is used when the current slot is a placeholder, but we're still within
    the FRUIT_PADDING_MINUTES window of a recent real event. Returns the event
    info so the deeplink stays active during the padding period.
    
    Returns dict with event info or None if no recent event found.
    """
    from datetime import datetime as _dt, timedelta, timezone
    
    padding_minutes = int(os.getenv('FRUIT_PADDING_MINUTES', '45'))
    
    # Parse at_ts to datetime
    try:
        now_dt = _dt.fromisoformat(at_ts.replace('Z', '+00:00'))
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
    except Exception:
        now_dt = _dt.now(timezone.utc)
    
    # Calculate padding window start
    padding_window_start = now_dt - timedelta(minutes=padding_minutes)
    
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT
                le.event_id,
                le.start_utc,
                le.end_utc,
                le.chosen_provider,
                e.title,
                e.channel_name,
                e.synopsis
            FROM lane_events le
            JOIN events e ON le.event_id = e.id
            WHERE le.lane_id = ?
              AND le.is_placeholder = 0
              AND datetime(le.end_utc) >= datetime(?)
              AND datetime(le.end_utc) <= datetime(?)
            ORDER BY le.end_utc DESC
            LIMIT 1
            """,
            (lane_id, padding_window_start.isoformat(), at_ts)
        )
        
        row = cur.fetchone()
        
        if row:
            return {
                'event_id': row['event_id'],
                'title': row['title'],
                'channel_name': row['channel_name'],
                'synopsis': row['synopsis'],
                'start_utc': row['start_utc'],
                'end_utc': row['end_utc'],
                'chosen_provider': row['chosen_provider'],
                'is_fallback': True
            }
        
        return None
        
    except Exception as e:
        log(f"Error in get_fallback_event_for_lane: {e}", "ERROR")
        return None

# ==================== Auto-refresh + Refresh Runner ====================
def schedule_auto_refresh_from_settings():
    """Create/refresh the APScheduler job based on current settings"""
    global auto_refresh_job, scheduler, auto_refresh_settings

    if not scheduler:
        return

    # Clear existing job
    if auto_refresh_job is not None:
        try:
            auto_refresh_job.remove()
        except Exception:
            pass
        auto_refresh_job = None

    if not auto_refresh_settings.get("enabled"):
        log("Auto-refresh disabled; no daily job scheduled", "INFO")
        return

    time_str = auto_refresh_settings.get("time", "02:30")
    try:
        hour, minute = [int(x) for x in time_str.split(":", 1)]
    except Exception:
        log(f"Invalid auto refresh time '{time_str}', disabling job", "ERROR")
        auto_refresh_settings["enabled"] = False
        return

    try:
        auto_refresh_job = scheduler.add_job(
            func=lambda: run_refresh(skip_scrape=False, source="auto"),
            trigger="cron",
            hour=hour,
            minute=minute,
            id="daily_auto_refresh",
            replace_existing=True,
        )
        log(
            f"Auto-refresh scheduled daily at {hour:02d}:{minute:02d} (scheduler local TZ)",
            "INFO",
        )
    except Exception as e:
        log(f"Failed to schedule auto-refresh: {e}", "ERROR")


def start_scheduler_if_available():
    """Start APScheduler in background for auto-refresh"""
    global scheduler, auto_refresh_settings

    if not APSCHEDULER_AVAILABLE:
        log("APScheduler not installed; auto-refresh disabled", "ERROR")
        return

    try:
        scheduler = BackgroundScheduler(timezone=os.getenv("TZ", "America/New_York"))
        scheduler.start()
        log("APScheduler scheduler started", "INFO")

        auto_refresh_settings = get_auto_refresh_settings()
        schedule_auto_refresh_from_settings()
    except Exception as e:
        log(f"Error starting APScheduler: {e}", "ERROR")


def run_refresh(skip_scrape=False, source="manual"):
    """
    Shared refresh runner for manual and scheduled runs.

    source: "manual" or "auto" (for status breakdown)
    """
    global refresh_status

    if refresh_status["running"]:
        log("Refresh requested but one is already running; skipping", "WARNING")
        return

    refresh_status["running"] = True
    refresh_status["current_step"] = "Starting refresh..."
    label = "Auto" if source == "auto" else "Manual"
    log(f"{label} refresh triggered (skip_scrape={skip_scrape})", "INFO")

    outcome = "error"

    try:
        cmd = ["python3", "-u", str(BIN_DIR / "daily_refresh.py")]
        if skip_scrape:
            cmd.append("--skip-scrape")

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        for line in process.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            log_buffer.append(line)
            # Try to surface step info like "[1/5] ..."
            if "[" in line and "/" in line and "]" in line:
                refresh_status["current_step"] = line.strip()

        process.wait()

        if process.returncode == 0:
            outcome = "success"
            log(f"{label} refresh completed successfully", "INFO")
        else:
            outcome = "failed"
            log(f"{label} refresh failed with code {process.returncode}", "ERROR")

    except Exception as e:
        outcome = "error"
        log(f"{label} refresh error: {e}", "ERROR")
    finally:
        refresh_status["running"] = False
        refresh_status["current_step"] = None
        now_iso = datetime.now().isoformat()

        # Overall last run
        refresh_status["last_run"] = now_iso
        refresh_status["last_status"] = outcome

        # Source-specific breakdown
        if source == "auto":
            refresh_status["last_run_auto"] = now_iso
            refresh_status["last_status_auto"] = outcome
        else:
            refresh_status["last_run_manual"] = now_iso
            refresh_status["last_status_manual"] = outcome


# ==================== CDVR Detector Functions ====================
def create_dummy_segment():
    """Create a simple black video segment using ffmpeg"""
    global DUMMY_SEGMENT_PATH
    
    try:
        temp_file = tempfile.NamedTemporaryFile(suffix='.ts', delete=False)
        DUMMY_SEGMENT_PATH = temp_file.name
        temp_file.close()
        
        log("Creating dummy video segment for HLS streams...", "INFO")
        
        result = subprocess.run([
            'ffmpeg', '-y',
            '-f', 'lavfi', '-i', 'color=black:s=1280x720:r=30',
            '-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=stereo',
            '-t', '60',
            '-c:v', 'libx264', '-preset', 'ultrafast',
            '-c:a', 'aac', '-b:a', '128k',
            '-pix_fmt', 'yuv420p',
            '-f', 'mpegts',
            DUMMY_SEGMENT_PATH
        ], check=True, capture_output=True, timeout=60)
        
        file_size = os.path.getsize(DUMMY_SEGMENT_PATH)
        log(f"Created dummy segment: {DUMMY_SEGMENT_PATH} ({file_size} bytes)", "INFO")
        return True
        
    except Exception as e:
        log(f"Could not create dummy segment: {e}", "WARN")
        log("Install ffmpeg: apt-get install ffmpeg", "WARN")
        return False

def bootstrap_streamlink_files():
    """Bootstrap streamlink files for CDVR detector on startup"""
    if not DETECTOR_ENABLED:
        log("CDVR Detector: Disabled (CDVR_DVR_PATH not set)", "INFO")
        return False
    
    log("CDVR Detector: Bootstrapping streamlink files...", "INFO")
    
    cdvr_url = f"http://{CDVR_SERVER_IP}:{CDVR_SERVER_PORT}"
    
    try:
        STREAMLINK_DIR.mkdir(parents=True, exist_ok=True)
        log(f"Streamlink directory ready: {STREAMLINK_DIR}", "INFO")
        
        placeholder_deeplink = "about:blank"
        files_created = 0
        
        for lane_num in range(1, NUM_LANES + 1):
            filepath = STREAMLINK_DIR / f"lane{lane_num}.strmlnk"
            
            if not filepath.exists():
                filepath.write_text(placeholder_deeplink)
                files_created += 1
        
        if files_created > 0:
            log(f"Created {files_created} new streamlink files", "INFO")
            log("Triggering CDVR scan to index files...", "INFO")
            
            scan_resp = requests.put(f"{cdvr_url}/dvr/scanner/scan", timeout=30)
            
            if scan_resp.status_code == 200:
                log("CDVR scan triggered, waiting for indexing...", "INFO")
                time.sleep(10)  # Wait for initial indexing
            else:
                log(f"CDVR scan failed: {scan_resp.status_code}", "WARN")
        else:
            log(f"All {NUM_LANES} streamlink files already exist", "INFO")
        
        # Hide FruitDeepLinks group from GUI
        try:
            groups_resp = requests.get(f"{cdvr_url}/dvr/groups?all=true", timeout=5)
            groups = groups_resp.json()
            
            group_id = None
            for group in groups:
                if group.get('Name') == 'FruitDeepLinks':
                    group_id = group.get('ID')
                    break
            
            if group_id:
                hide_resp = requests.put(f"{cdvr_url}/dvr/groups/{group_id}/visibility/hidden", timeout=5)
                if hide_resp.status_code == 200:
                    log("Hidden FruitDeepLinks group from CDVR GUI", "INFO")
        except Exception:
            pass
        
        log("CDVR Detector bootstrap complete", "INFO")
        return True
        
    except Exception as e:
        log(f"Bootstrap error: {e}", "ERROR")
        return False

def get_deeplink_for_lane(lane_number: int, self_base_url: str, deeplink_format: str = "scheme") -> dict:
    """Get deeplink for the current event on this lane from local whatson API.

    Args:
        lane_number: Lane ID to query
        self_base_url: Base URL like http://127.0.0.1:6655 (no trailing slash)
        deeplink_format: 'scheme' for Apple TV (default), 'http' for Android/Fire TV
    """
    try:
        base = (self_base_url or "").rstrip("/")
        if not base:
            base = f"http://127.0.0.1:{int(os.getenv('PORT', 6655))}"

        api_url = f"{base}/whatson/{lane_number}?include=deeplink&deeplink_format={deeplink_format}"
        resp = requests.get(api_url, timeout=3)

        if resp.status_code != 200:
            return None

        data = resp.json() if resp.content else None
        if not data or not data.get("ok") or not data.get("event_uid"):
            return None

        deeplink = data.get("deeplink_url") or data.get("deeplink_url_full")
        if not deeplink:
            return None

        title = data.get("title") or f"Lane {lane_number} Event"
        event_uid = data.get("event_uid")
        is_fallback = data.get("is_fallback", False)

        # Log if we're using a fallback deeplink
        if is_fallback:
            log(f"get_deeplink_for_lane: Lane {lane_number} using FALLBACK deeplink for '{title}'", "INFO")

        return {
            "deeplink": deeplink,
            "title": title,
            "event_uid": event_uid,
            "event_data": data,
            "is_fallback": is_fallback,
        }

    except Exception:
        return None

def trigger_playback_on_client(client_ip: str, deeplink: str, lane_number: int) -> dict:
    """Orchestrate playback: update strmlnk, reprocess, trigger client"""
    result = {
        "lane_number": lane_number,
        "strm_updated": False,
        "file_id": None,
        "cdvr_reprocessed": False,
        "recording_id": None,
        "playback_triggered": False
    }
    
    try:
        log(f"Triggering playback for lane {lane_number} on {client_ip}", "INFO")
        log(f"Deeplink to use: {deeplink}", "INFO")
        
        # Update streamlink file
        strmlnk_path = STREAMLINK_DIR / f"lane{lane_number}.strmlnk"
        strmlnk_path.write_text(deeplink)
        result["strm_updated"] = True
        log(f"Updated streamlink file: {strmlnk_path}", "INFO")
        
        # Get file ID from CDVR
        cdvr_url = f"http://{CDVR_SERVER_IP}:{CDVR_SERVER_PORT}"
        files_resp = requests.get(f"{cdvr_url}/dvr/files", timeout=10)
        files = files_resp.json()
        
        lane_filename = f"FruitDeepLinks/lane{lane_number}.strmlnk"
        file_id = None
        
        for file_obj in files:
            if file_obj.get('Path') == lane_filename:
                file_id = file_obj.get('ID')
                result["file_id"] = file_id
                result["recording_id"] = file_id
                break
        
        if not file_id:
            result["error"] = f"File {lane_filename} not found in CDVR"
            log(f"ERROR: File not found in CDVR: {lane_filename}", "ERROR")
            return result
        
        log(f"Found CDVR file ID: {file_id}", "INFO")
        
        # Reprocess file (fast method from SLM)
        reprocess_resp = requests.put(f"{cdvr_url}/dvr/files/{file_id}/reprocess", timeout=10)
        
        if reprocess_resp.status_code == 200:
            result["cdvr_reprocessed"] = True
            log(f"Reprocessed file successfully", "INFO")
            time.sleep(2)
        else:
            result["error"] = f"Reprocess returned {reprocess_resp.status_code}"
            log(f"ERROR: Reprocess failed with status {reprocess_resp.status_code}", "ERROR")
            return result
        
        # Trigger playback on client
        play_url = f"http://{client_ip}:{CDVR_API_PORT}/api/play/recording/{file_id}"
        log(f"Triggering play at: {play_url}", "INFO")
        play_resp = requests.post(play_url, timeout=5)
        
        if play_resp.status_code == 200:
            result["playback_triggered"] = True
            log(f"Successfully triggered deeplink on {client_ip}", "INFO")
        else:
            log(f"Play request returned status {play_resp.status_code}", "WARN")
        
        return result
        
    except Exception as e:
        log(f"Playback trigger error: {e}", "ERROR")
        result["error"] = str(e)
        return result

def auto_detect_and_trigger(lane_number: int, hint_client_ip: str, self_base_url: str):
    """Auto-detection that polls CDVR clients and triggers deeplink playback.

    Notes:
      - The HLS request usually comes from the CDVR server, not the end device.
      - Some installs report connected=false even while recently seen, so we use recency.
    """
    try:
        cdvr_clients_url = f"http://{CDVR_SERVER_IP}:{CDVR_SERVER_PORT}/dvr/clients/info"
        resp = requests.get(cdvr_clients_url, timeout=5)
        if resp.status_code != 200:
            log(f"Detector: /dvr/clients/info returned {resp.status_code}", "WARN")
            return

        clients = resp.json() or []
        log(f"Detector: clients returned={len(clients)} hint_ip={hint_client_ip}", "INFO")

        def has_api_support(platform_str: str) -> bool:
            if not platform_str:
                return False
            p = platform_str.lower()
            return any(x in p for x in ["tvos", "firetv", "androidtv", "android"])

        now_ms = int(time.time() * 1000)
        recent = []
        for c in clients:
            if not has_api_support(c.get("platform", "")):
                continue
            seen_at = c.get("seen_at")
            try:
                age_ms = now_ms - int(seen_at)
            except Exception:
                age_ms = 999999999
            if age_ms <= 90_000:
                recent.append((age_ms, c))

        candidates = [c for (_age, c) in sorted(recent, key=lambda t: t[0])] if recent else [
            c for c in clients if has_api_support(c.get("platform", ""))
        ]

        log(f"Detector: candidates={len(candidates)} (recent={len(recent)})", "INFO")

        for client in candidates:
            client_ip = client.get("local_ip")
            if not client_ip:
                continue

            try:
                status_url = f"http://{client_ip}:{CDVR_API_PORT}/api/status"
                status_resp = requests.get(status_url, timeout=3)
                if status_resp.status_code != 200:
                    continue
                status = status_resp.json() or {}

                if status.get("status") != "playing":
                    continue

                channel = status.get("channel") or {}
                ch_name = channel.get("name") or ""

                import re
                lane_match = re.search(r"(\d+)\s*$", ch_name)
                if not lane_match:
                    continue

                detected_lane = int(lane_match.group(1))
                if detected_lane != int(lane_number):
                    continue

                log(f"Detector: matched lane={lane_number} client={client_ip} ch='{ch_name}'", "INFO")
                
                # Detect platform and choose deeplink format
                platform = client.get("platform", "").lower()
                is_android = any(x in platform for x in ["android", "firetv"])
                deeplink_format = "http" if is_android else "scheme"
                
                log(f"Detector: client platform={platform}, using deeplink_format={deeplink_format}", "INFO")

                deeplink_info = get_deeplink_for_lane(int(lane_number), self_base_url, deeplink_format)
                if not deeplink_info:
                    log(f"Detector: no deeplink found for lane={lane_number}", "WARN")
                    return

                deeplink = deeplink_info.get("deeplink")
                result = trigger_playback_on_client(client_ip, deeplink, int(lane_number))
                
                return

            except Exception as e:
                log(f"Detector: error checking client {client_ip}: {e}", "WARN")
                continue

        log(f"Detector: no matching playing client found for lane={lane_number}", "INFO")

    except Exception as e:
        log(f"Detector: fatal error: {e}", "ERROR")



# ==================== File Serving / API ====================
@app.route("/")
def index():
    """Admin dashboard"""
    return load_template("admin_dashboard.html")



@app.route("/api")
def api_helper():
    """Simple HTML API helper page"""
    return load_template("api_helper.html")

@app.route("/adb")
def adb_config_page():
    """ADB configuration page"""
    return load_template("adb_config.html")

@app.route("/api/status")
def api_status():
    """Get system status"""
    stats = get_db_stats()

    # File info
    files = {}
    for file_path in list(OUT_DIR.glob("*.xml")) + list(OUT_DIR.glob("*.m3u")):
        if file_path.exists():
            stat = file_path.stat()
            files[file_path.name] = {
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }

    # Auto-refresh status snapshot
    auto_settings = get_auto_refresh_settings()
    next_run = None
    if auto_refresh_job and auto_settings.get("enabled"):
        try:
            next_run = auto_refresh_job.next_run_time.isoformat()
        except Exception:
            next_run = None

    # Environment variables (for display on admin page)
    env_vars = {
        "SERVER_URL": os.getenv("SERVER_URL", ""),
        "FRUIT_HOST_PORT": os.getenv("FRUIT_HOST_PORT", ""),
        "CHANNELS_DVR_IP": os.getenv("CHANNELS_DVR_IP", ""),
        "CHANNELS_SOURCE_NAME": os.getenv("CHANNELS_SOURCE_NAME", ""),
        "CDVR_DVR_PATH": os.getenv("CDVR_DVR_PATH", ""),
        "CDVR_SERVER_PORT": os.getenv("CDVR_SERVER_PORT", ""),
        "CDVR_API_PORT": os.getenv("CDVR_API_PORT", ""),
        "TZ": os.getenv("TZ", ""),
        "FRUIT_LANES": os.getenv("FRUIT_LANES", ""),
        "FRUIT_LANE_START_CH": os.getenv("FRUIT_LANE_START_CH", ""),
        "AUTO_REFRESH_ENABLED": os.getenv("AUTO_REFRESH_ENABLED", ""),
        "AUTO_REFRESH_TIME": os.getenv("AUTO_REFRESH_TIME", ""),
        "HEADLESS": os.getenv("HEADLESS", ""),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", ""),
    }
    # Filter out empty values
    env_vars = {k: v for k, v in env_vars.items() if v}

    return jsonify(
        {
            "status": "online",
            "database": stats,
            "files": files,
            "refresh": refresh_status,
            "auto_refresh": {
                "enabled": auto_settings.get("enabled", False),
                "time": auto_settings.get("time", "02:30"),
                "next_run": next_run,
            },
            "env_vars": env_vars,
            "timestamp": datetime.now().isoformat(),
        }
    )


@app.route("/api/logs")
def api_logs():
    """Get recent logs"""
    count = request.args.get("count", 100, type=int)
    return jsonify({"logs": list(log_buffer)[-count:], "count": len(log_buffer)})


@app.route("/api/logs/stream")
def api_logs_stream():
    """Stream logs in real-time (SSE)"""

    def generate():
        last_index = len(log_buffer)
        while True:
            current_index = len(log_buffer)
            if current_index > last_index:
                for log_line in list(log_buffer)[last_index:]:
                    yield f"data: {json.dumps({'log': log_line})}\n\n"
                last_index = current_index
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream")


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a manual refresh"""
    if refresh_status["running"]:
        return jsonify({"error": "Refresh already running"}), 409

    skip_scrape = request.json.get("skip_scrape", False) if request.json else False

    # Run in a background thread so the HTTP request returns immediately
    thread = threading.Thread(
        target=lambda: run_refresh(skip_scrape=skip_scrape, source="manual"),
        daemon=True,
    )
    thread.start()
    return jsonify({"status": "started"})


@app.route("/api/auto-refresh", methods=["GET", "POST"])
def api_auto_refresh():
    """Get or update auto-refresh scheduler settings"""
    global auto_refresh_settings

    if request.method == "GET":
        auto_refresh_settings = get_auto_refresh_settings()
        next_run = None
        if auto_refresh_job and auto_refresh_settings.get("enabled"):
            try:
                next_run = auto_refresh_job.next_run_time.isoformat()
            except Exception:
                next_run = None

        return jsonify(
            {
                "enabled": auto_refresh_settings.get("enabled", False),
                "time": auto_refresh_settings.get("time", "02:30"),
                "next_run": next_run,
            }
        )

    data = request.json or {}
    enabled = bool(data.get("enabled", False))
    time_str = data.get("time", "02:30")

    # Validate HH:MM
    try:
        hour, minute = [int(x) for x in time_str.split(":", 1)]
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        time_str = f"{hour:02d}:{minute:02d}"
    except Exception:
        return jsonify({"error": "Invalid time format, use HH:MM (24h)"}), 400

    auto_refresh_settings = {"enabled": enabled, "time": time_str}

    if not save_auto_refresh_settings(auto_refresh_settings):
        return jsonify({"error": "Failed to save settings"}), 500

    schedule_auto_refresh_from_settings()

    next_run = None
    if auto_refresh_job and auto_refresh_settings.get("enabled"):
        try:
            next_run = auto_refresh_job.next_run_time.isoformat()
        except Exception:
            next_run = None

    return jsonify(
        {
            "enabled": auto_refresh_settings["enabled"],
            "time": auto_refresh_settings["time"],
            "next_run": next_run,
        }
    )


@app.route("/api/apply-filters", methods=["POST"])
def api_apply_filters():
    """Apply current filter settings by regenerating exports only"""
    if refresh_status["running"]:
        return jsonify({"error": "Refresh already running"}), 409

    def run_apply_filters():
        refresh_status["running"] = True
        refresh_status["current_step"] = "Applying filters..."
        log("Applying filter settings (regenerating exports)", "INFO")

        try:
            # Only run export scripts, skip scraping/importing
            scripts = [
                (
                    "fruit_build_lanes.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "fruit_build_lanes.py"),
                        "--db",
                        str(DB_PATH),
                        "--lanes",
                        os.getenv("FRUIT_LANES", os.getenv("PEACOCK_LANES", "50")),
                    ],
                ),
                (
                    "fruit_export_hybrid.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "fruit_export_hybrid.py"),
                        "--db",
                        str(DB_PATH),
                    ],
                ),
                (
                    "fruit_export_lanes.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "fruit_export_lanes.py"),
                        "--db",
                        str(DB_PATH),
                        "--server-url",
                        os.getenv("SERVER_URL", "http://192.168.86.80:6655"),
                    ],
                ),
            ]

            for script_name, cmd in scripts:
                refresh_status["current_step"] = f"Running {script_name}..."
                log(f"Running {script_name}", "INFO")

                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                for line in process.stdout:
                    log_buffer.append(line.strip())

                process.wait()

                if process.returncode != 0:
                    raise Exception(f"{script_name} failed with code {process.returncode}")

            refresh_status["last_status"] = "success"
            log("Filters applied successfully!", "INFO")

        except Exception as e:
            refresh_status["last_status"] = "error"
            log(f"Apply filters error: {str(e)}", "ERROR")

        finally:
            refresh_status["running"] = False
            refresh_status["current_step"] = None
            now_iso = datetime.now().isoformat()
            refresh_status["last_run"] = now_iso

    thread = threading.Thread(target=run_apply_filters, daemon=True)
    thread.start()
    return jsonify({"status": "started"})


# ==================== Filters APIs ====================
@app.route("/filters")
def filters_page():
    """Filters configuration page"""
    return load_template("filters.html")


@app.route("/api/filters")
def api_filters():
    """Get available filters (providers, sports, leagues)"""
    filters = get_available_filters()
    prefs = get_user_preferences()
    return jsonify({"filters": filters, "preferences": prefs})


@app.route("/api/filters/priorities", methods=["GET", "POST"])
def api_filter_priorities():
    """Get or update service priority order and Amazon penalty setting"""
    if request.method == "GET":
        prefs = get_user_preferences()
        return jsonify({
            "service_priorities": prefs.get("service_priorities", {}),
            "amazon_penalty": prefs.get("amazon_penalty", True)
        })
    
    elif request.method == "POST":
        data = request.json or {}
        prefs = get_user_preferences()
        
        # Update priorities if provided
        if "service_priorities" in data:
            prefs["service_priorities"] = data["service_priorities"]
        
        # Update Amazon penalty if provided
        if "amazon_penalty" in data:
            prefs["amazon_penalty"] = bool(data["amazon_penalty"])
        
        if save_user_preferences(prefs):
            log("Service priorities updated", "INFO")
            return jsonify({"status": "success"})
        else:
            return jsonify({"status": "error", "message": "Failed to save priorities"}), 500


@app.route("/api/filters/selection-examples")
def api_selection_examples():
    """Get sample events showing which services would be selected"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database not found"}), 500
    
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get user preferences
        if FILTERING_AVAILABLE:
            prefs = load_user_preferences(conn)
        else:
            prefs = get_user_preferences()
        
        enabled_services = prefs.get("enabled_services", [])
        priority_map = prefs.get("service_priorities", {})
        amazon_penalty = prefs.get("amazon_penalty", True)
        
        # Find events with multiple DISTINCT services (more interesting for examples)
        cur.execute("""
            SELECT e.id, e.title, e.channel_name, e.start_utc,
                   COUNT(DISTINCT p.logical_service) as service_count
            FROM events e
            JOIN playables p ON e.id = p.event_id
            WHERE datetime(e.end_utc) > datetime('now')
              AND p.logical_service IS NOT NULL
            GROUP BY e.id
            HAVING service_count > 1
            ORDER BY service_count DESC, e.start_utc ASC
            LIMIT 10
        """)
        
        examples = []
        for row in cur.fetchall():
            event_id = row["id"]
            
            # Get all playables for this event
            if FILTERING_AVAILABLE:
                from filter_integration import get_filtered_playables, get_service_display_name
                
                # Query ALL playables directly from DB to show what's available
                cur.execute("""
                    SELECT DISTINCT provider, deeplink_play, deeplink_open, playable_url
                    FROM playables
                    WHERE event_id = ?
                """, (event_id,))
                
                # Determine logical service for each unique playable
                seen_services = {}
                for prow in cur.fetchall():
                    # Import here to avoid circular imports
                    try:
                        from logical_service_mapper import get_logical_service_for_playable
                        logical_service = get_logical_service_for_playable(
                            provider=prow[0],
                            deeplink_play=prow[1],
                            deeplink_open=prow[2],
                            playable_url=prow[3],
                            event_id=event_id,
                            conn=conn
                        )
                    except ImportError:
                        logical_service = prow[0]  # Fallback to provider
                    
                    if logical_service not in seen_services:
                        seen_services[logical_service] = {
                            "code": logical_service,
                            "name": get_service_display_name(logical_service),
                            "priority": priority_map.get(logical_service, 50)
                        }
                
                available_services = list(seen_services.values())
                # Sort by priority for better display
                available_services.sort(key=lambda s: -s["priority"])
                
                # Get filtered playables (with user preferences) to determine winner
                filtered_playables = get_filtered_playables(
                    conn, event_id, enabled_services, priority_map, amazon_penalty
                )
                
                winner = filtered_playables[0] if filtered_playables else None
                winner_info = None
                reason = "No enabled services match"
                
                if winner:
                    winner_code = winner["logical_service"]
                    winner_priority = priority_map.get(winner_code, 50)
                    winner_info = {
                        "code": winner_code,
                        "name": get_service_display_name(winner_code),
                        "priority": winner_priority
                    }
                    
                    # Build reason
                    if len(filtered_playables) == 1:
                        reason = f"Only enabled service available"
                    else:
                        reason = f"Highest priority ({winner_priority}) among enabled services"
                        
                        # Check if Amazon penalty applied
                        has_amazon = any(s["code"] == "aiv" for s in available_services)
                        has_non_amazon = any(s["code"] != "aiv" for s in available_services)
                        if amazon_penalty and has_amazon and has_non_amazon and winner_code != "aiv":
                            reason += " (Amazon deprioritized)"
            else:
                # Fallback without filter_integration
                available_services = []
                winner_info = None
                reason = "Filter integration not available"
            
            examples.append({
                "title": row["title"],
                "channel": row["channel_name"],
                "start_utc": row["start_utc"],
                "available_services": available_services,
                "selected_service": winner_info,
                "reason": reason
            })
        
        conn.close()
        return jsonify({
            "examples": examples,
            "preferences": {
                "enabled_services": enabled_services,
                "amazon_penalty": amazon_penalty
            }
        })
        
    except Exception as e:
        log(f"Error in selection-examples: {e}", "ERROR")
        return jsonify({"error": str(e)}), 500


@app.route("/api/filters/preferences", methods=["GET", "POST"])
def api_filters_preferences():
    """Get or update user filter preferences"""
    if request.method == "GET":
        return jsonify(get_user_preferences())

    elif request.method == "POST":
        prefs = request.json
        if save_user_preferences(prefs):
            log("Filter preferences updated", "INFO")
            return jsonify({"status": "success"})
        else:
            return (
                jsonify({"status": "error", "message": "Failed to save preferences"}),
                500,
            )




# ==================== Provider Lanes API ====================
def get_provider_lane_stats(conn: sqlite3.Connection) -> list[dict]:
    """
    Get comprehensive stats for each logical service for ADB lane configuration.
    
    Returns list of dicts with:
    {
        "provider_code": "watchtnt",
        "name": "TNT",
        "event_count": 45,        # Unique events with this service
        "playable_count": 52,     # Total playables (may have duplicates)
        "future_event_count": 30, # Events that haven't ended yet
        "adb_enabled": 0,         # From provider_lanes table
        "adb_lane_count": 0       # From provider_lanes table
    }
    """
    cur = conn.cursor()
    
    # Get stats from playables grouped by logical_service
    # Check if logical_service column exists first
    cur.execute("PRAGMA table_info(playables)")
    columns = [row[1] for row in cur.fetchall()]
    
    if 'logical_service' in columns:
        # Use stored logical_service column
        cur.execute("""
            SELECT 
                p.logical_service as service_code,
                COUNT(DISTINCT p.event_id) as event_count,
                COUNT(*) as playable_count,
                COUNT(DISTINCT CASE 
                    WHEN datetime(e.end_utc) > datetime('now') 
                    THEN p.event_id 
                END) as future_event_count
            FROM playables p
            LEFT JOIN events e ON p.event_id = e.id
            WHERE p.logical_service IS NOT NULL
              AND p.logical_service != ''
            GROUP BY p.logical_service
            ORDER BY event_count DESC
        """)
    else:
        # Fallback: use provider column
        cur.execute("""
            SELECT 
                p.provider as service_code,
                COUNT(DISTINCT p.event_id) as event_count,
                COUNT(*) as playable_count,
                COUNT(DISTINCT CASE 
                    WHEN datetime(e.end_utc) > datetime('now') 
                    THEN p.event_id 
                END) as future_event_count
            FROM playables p
            LEFT JOIN events e ON p.event_id = e.id
            WHERE p.provider IS NOT NULL
              AND p.provider != ''
            GROUP BY p.provider
            ORDER BY event_count DESC
        """)
    
    services = {}
    for row in cur.fetchall():
        service_code = row[0]
        
        # Get display name
        display_name = service_code
        if LOGICAL_SERVICES_AVAILABLE:
            try:
                from logical_service_mapper import get_service_display_name
                display_name = get_service_display_name(service_code)
            except:
                display_name = service_code.upper()
        
        services[service_code] = {
            "provider_code": service_code,
            "name": display_name,
            "event_count": row[1],
            "playable_count": row[2],
            "future_event_count": row[3],
            "adb_enabled": 0,
            "adb_lane_count": 0,
            "created_at": None,
            "updated_at": None
        }
    
    # Merge with provider_lanes configuration
    cur.execute("""
        SELECT provider_code, adb_enabled, adb_lane_count, created_at, updated_at
        FROM provider_lanes
    """)
    
    for row in cur.fetchall():
        code = row[0]
        if code in services:
            services[code]["adb_enabled"] = row[1]
            services[code]["adb_lane_count"] = row[2]
            services[code]["created_at"] = row[3]
            services[code]["updated_at"] = row[4]
        else:
            # Service in provider_lanes but no playables (could be old/disabled)
            display_name = code
            if LOGICAL_SERVICES_AVAILABLE:
                try:
                    from logical_service_mapper import get_service_display_name
                    display_name = get_service_display_name(code)
                except:
                    display_name = code.upper()
            
            services[code] = {
                "provider_code": code,
                "name": display_name,
                "event_count": 0,
                "playable_count": 0,
                "future_event_count": 0,
                "adb_enabled": row[1],
                "adb_lane_count": row[2],
                "created_at": row[3],
                "updated_at": row[4]
            }
    
    # Sort by event count (descending), then by name
    return sorted(services.values(), key=lambda x: (-x["event_count"], x["name"]))


@app.route("/api/provider_lanes", methods=["GET", "POST"])
def api_provider_lanes():
    """
    Get or update per-provider ADB lane configuration.

    GET: returns list of providers with ADB flags.
    POST: expects JSON array or {"providers": [...]}.
    """
    conn = get_db_connection()
    if conn is None:
        return (
            jsonify({"status": "error", "message": "Database not found"}),
            500,
        )

    conn.row_factory = sqlite3.Row
    try:
        if request.method == "GET":
            # Use enhanced stats function that shows event counts
            try:
                providers = get_provider_lane_stats(conn)
                return jsonify({"status": "success", "providers": providers})
            except Exception as e:
                log(f"Error getting provider lane stats: {e}", "ERROR")
                # Fallback to old behavior
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT provider_code, adb_enabled, adb_lane_count,
                           created_at, updated_at
                      FROM provider_lanes
                     ORDER BY provider_code
                    """
                )
                rows = [dict(row) for row in cur.fetchall()]
                return jsonify({"status": "success", "providers": rows})

        # POST
        payload = request.get_json(silent=True) or {}
        providers = payload.get("providers")
        if providers is None and isinstance(payload, list):
            providers = payload

        if not isinstance(providers, list):
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Expected JSON list or {\"providers\": [...]} payload",
                    }
                ),
                400,
            )

        updated = 0
        cur = conn.cursor()
        for item in providers:
            if not isinstance(item, dict):
                continue
            code = (item.get("provider_code") or "").strip()
            if not code:
                continue

            # Normalize values
            enabled_raw = item.get("adb_enabled")
            lane_raw = item.get("adb_lane_count", 0)

            adb_enabled = 1 if enabled_raw in (1, True, "1", "true", "True") else 0
            try:
                adb_lane_count = int(lane_raw or 0)
            except (TypeError, ValueError):
                adb_lane_count = 0

            cur.execute(
                """
                INSERT INTO provider_lanes (provider_code, adb_enabled, adb_lane_count, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                ON CONFLICT(provider_code) DO UPDATE SET
                    adb_enabled = excluded.adb_enabled,
                    adb_lane_count = excluded.adb_lane_count,
                    updated_at = datetime('now')
                """,
                (code, adb_enabled, adb_lane_count),
            )
            updated += 1

        conn.commit()
        log(f"Updated provider_lanes for {updated} provider(s)", "INFO")
        return jsonify({"status": "success", "updated": updated})
    finally:
        conn.close()


@app.route("/api/adb/lanes/<provider_code>/<int:lane_number>/deeplink")
def api_adb_lane_deeplink(provider_code, lane_number):
    """
    Get the current deeplink for a specific provider's ADB lane.
    
    This endpoint queries the adb_lanes table to find what event is currently
    scheduled on the given provider's lane, then retrieves the deeplink.
    
    Query Parameters:
    - format: 'text' (default) or 'json'
    - deeplink_format: 'scheme' (default) or 'http' (for Android/Fire TV)
    - at: ISO timestamp (default: now)
    
    Examples:
    - /api/adb/lanes/sportscenter/1/deeplink?format=text
    - /api/adb/lanes/pplus/3/deeplink?format=json
    - /api/adb/lanes/aiv/2/deeplink?format=json&deeplink_format=http
    
    Returns:
    - Text format: Just the deeplink URL (e.g., "sportscenter://...")
    - JSON format: {"deeplink": "...", "title": "...", "channel_id": "..."}
    """
    conn = get_db_connection()
    if conn is None:
        if request.args.get("format") == "text":
            return Response("", mimetype="text/plain")
        return jsonify({"status": "error", "message": "Database not found"}), 404
    
    conn.row_factory = sqlite3.Row
    
    try:
        from datetime import datetime as _dt
        
        # Get timestamp to query
        at_ts = request.args.get("at")
        if not at_ts:
            at_ts = _dt.utcnow().isoformat(timespec="seconds")
        
        # Get deeplink format preference
        deeplink_format = request.args.get("deeplink_format", "scheme").lower()
        
        cur = conn.cursor()
        
        # Query adb_lanes table for current event on this provider+lane
        cur.execute(
            """
            SELECT 
                event_id,
                channel_id,
                start_utc,
                stop_utc
            FROM adb_lanes
            WHERE provider_code = ?
              AND lane_number = ?
              AND datetime(start_utc) <= datetime(?)
              AND datetime(stop_utc) > datetime(?)
            ORDER BY start_utc DESC
            LIMIT 1
            """,
            (provider_code, lane_number, at_ts, at_ts)
        )
        
        adb_row = cur.fetchone()
        
        if not adb_row:
            # No event scheduled for this provider+lane at this time
            if request.args.get("format") == "text":
                return Response("", mimetype="text/plain")
            return jsonify({
                "status": "success",
                "deeplink": None,
                "title": None,
                "provider_code": provider_code,
                "lane_number": lane_number,
                "message": "No event scheduled at this time"
            })
        
        event_id_str = adb_row["event_id"]
        channel_id = adb_row["channel_id"]
        start_utc = adb_row["start_utc"]
        stop_utc = adb_row["stop_utc"]
        
        # Get event details from events table
        # The event_id in adb_lanes has an "appletv-" prefix that needs to be stripped
        # to match the pvid in the events table
        
        # Strip "appletv-" prefix if present
        event_lookup_id = event_id_str
        if event_lookup_id.startswith("appletv-"):
            event_lookup_id = event_lookup_id[8:]  # Remove "appletv-" (8 characters)
        
        # First, determine which column in events table matches our event_id
        uid_col, primary_col, full_col = get_event_link_columns(conn)
        
        log(f"ADB deeplink lookup: provider={provider_code}, lane={lane_number}, event_id={event_id_str}, lookup_id={event_lookup_id}, uid_col={uid_col}", "DEBUG")
        
        # Try to find the event by UID
        cur.execute(
            f"""
            SELECT 
                id,
                title,
                channel_name,
                synopsis,
                start_utc,
                end_utc
            FROM events
            WHERE {uid_col} = ?
            LIMIT 1
            """,
            (event_lookup_id,)
        )
        
        event_row = cur.fetchone()
        
        if not event_row:
            log(f"ADB deeplink: Event not found in events table for {uid_col}={event_lookup_id}", "DEBUG")
        
        if not event_row:
            # Event not found in events table, return just the event_id
            if request.args.get("format") == "text":
                # Return the event_id as a basic deeplink
                return Response(event_id_str, mimetype="text/plain")
            return jsonify({
                "status": "success",
                "deeplink": event_id_str,
                "title": None,
                "provider_code": provider_code,
                "lane_number": lane_number,
                "channel_id": channel_id,
                "start_utc": start_utc,
                "stop_utc": stop_utc,
                "message": "Event details not found, returning event_id"
            })
        
        # Get deeplink for this event
        db_event_id = event_row["id"]
        link_info = get_event_link_info(conn, db_event_id, uid_col, primary_col, full_col)
        deeplink_url = link_info.get("deeplink_url") or link_info.get("deeplink_url_full") or event_id_str
        
        # Convert to HTTP format if requested (for Android/Fire TV)
        if deeplink_format == "http" and deeplink_url:
            try:
                from deeplink_converter import generate_http_deeplink
                playable_uuid = get_playable_id_for_event(conn, db_event_id, provider_code)
                try:
                    http_version = generate_http_deeplink(
                        deeplink_url,
                        provider=provider_code,
                        playable_id=playable_uuid,
                    )
                except TypeError:
                    http_version = generate_http_deeplink(deeplink_url, provider_code)
                if http_version:
                    deeplink_url = http_version
            except ImportError:
                log("deeplink_converter not available for HTTP conversion", "WARN")
        
        # Return response
        fmt = (request.args.get("format") or "text").lower()
        if fmt == "text":
            return Response(deeplink_url or "", mimetype="text/plain")
        else:
            return jsonify({
                "status": "success",
                "deeplink": deeplink_url,
                "title": event_row["title"],
                "channel_name": event_row["channel_name"],
                "provider_code": provider_code,
                "lane_number": lane_number,
                "channel_id": channel_id,
                "start_utc": start_utc,
                "stop_utc": stop_utc,
                "event_start_utc": event_row["start_utc"],
                "event_end_utc": event_row["end_utc"],
                "deeplink_format": deeplink_format
            })
    
    except Exception as e:
        log(f"Error in api_adb_lane_deeplink: {e}", "ERROR")
        if request.args.get("format") == "text":
            return Response("", mimetype="text/plain")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()


# ==================== File Downloads ====================
@app.route("/out/<filename>")
def serve_file(filename):
    """Serve generated files (XMLTV/M3U)"""
    file_path = OUT_DIR / filename
    if not file_path.exists():
        return jsonify({"error": "File not found"}), 404

    return send_file(str(file_path), as_attachment=False)


@app.route("/xmltv/lanes")
def serve_lanes_xmltv():
    """Serve lanes XMLTV guide"""
    return send_file(str(OUT_DIR / "peacock_lanes.xml"))


@app.route("/m3u/lanes")
def serve_lanes_m3u():
    """Serve lanes M3U playlist"""
    return send_file(str(OUT_DIR / "peacock_lanes.m3u"))


@app.route("/xmltv/direct")
def serve_direct_xmltv():
    """Serve direct XMLTV guide"""
    return send_file(str(OUT_DIR / "direct.xml"))


@app.route("/m3u/direct")
def serve_direct_m3u():
    """Serve direct M3U playlist"""
    return send_file(str(OUT_DIR / "direct.m3u"))


# ==================== Stream Proxying (Future) ====================
@app.route("/lanes/<int:lane_id>/stream.m3u8")
def lane_stream(lane_id):
    """
    Stream endpoint for a lane
    TODO: Implement actual stream proxying based on current schedule
    """

    return (
        jsonify(
            {
                "error": "Stream proxying not yet implemented",
                "lane_id": lane_id,
                "message": "Use direct deeplinks for now",
            }
        ),
        501,
    )


@app.route("/api/lanes")
def api_lanes():
    """List all lanes with basic counts and current event (if any)."""
    if not DB_PATH.exists():
        return jsonify({"error": "Database not found"}), 500

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Event counts per lane (future + ongoing events)
        cur.execute(
            """
            SELECT le.lane_id, COUNT(*) AS event_count
            FROM lane_events le
            JOIN events e ON le.event_id = e.id
            WHERE datetime(e.end_utc) >= datetime('now')
            GROUP BY le.lane_id
            ORDER BY le.lane_id
            """
        )
        lane_rows = cur.fetchall()

        # Current events snapshot
        current_by_lane = get_current_events_by_lane(conn)

        lanes = []
        for row in lane_rows:
            lane_id = row["lane_id"]
            lane_info = {
                "lane_id": lane_id,
                "event_count": row["event_count"],
                "current": current_by_lane.get(lane_id),
            }
            lanes.append(lane_info)

        conn.close()
        return jsonify(lanes)

    except Exception as e:
        log(f"/api/lanes error: {e}", "ERROR")
        return jsonify({"error": str(e)}), 500


@app.route("/whatson/<int:lane_id>")
def whatson_lane(lane_id):
    """Get what's currently playing on a specific lane.

    JSON (default):
      GET /whatson/1
      GET /whatson/1?include=deeplink
      GET /whatson/1?deeplink=1
      GET /whatson/1?dynamic=1
      GET /whatson/1?deeplink_format=http  (for Android/Fire TV)
      GET /whatson/1?deeplink_format=scheme (for Apple TV, default)

    Plain text deeplink:
      GET /whatson/1?format=txt&param=deeplink_url
      GET /whatson/1?format=txt&param=deeplink_url&deeplink_format=http
    """
    if not DB_PATH.exists():
        if request.args.get("format") == "txt":
            return Response("", mimetype="text/plain")
        return jsonify({"ok": False, "error": "Database not found"}), 500

    from datetime import datetime as _dt

    at_ts = request.args.get("at")
    if not at_ts:
        at_ts = _dt.utcnow().isoformat(timespec="seconds")

    want_deeplink = False
    include_param = request.args.get("include")
    if include_param == "deeplink":
        want_deeplink = True
    if request.args.get("deeplink") in ("1", "true", "yes"):
        want_deeplink = True
    if request.args.get("dynamic") in ("1", "true", "yes"):
        want_deeplink = True
    
    # Deeplink format: 'http' for Android/Fire TV, 'scheme' for Apple TV (default)
    deeplink_format = request.args.get("deeplink_format", "scheme").lower()

    fmt = (request.args.get("format") or "json").lower()
    param = request.args.get("param") or "event_uid"

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Find the single current event for this lane at the given time
        cur.execute(
            """
            SELECT
                le.event_id,
                le.start_utc,
                le.end_utc,
                le.is_placeholder,
                le.chosen_provider,
                e.title,
                e.channel_name,
                e.synopsis
            FROM lane_events le
            LEFT JOIN events e ON le.event_id = e.id
            WHERE le.lane_id = ?
              AND datetime(le.start_utc) <= datetime(?)
              AND datetime(le.end_utc) > datetime(?)
            ORDER BY le.start_utc DESC
            LIMIT 1
            """,
            (lane_id, at_ts, at_ts),
        )
        row = cur.fetchone()

        uid_col, primary_col, full_col = get_event_link_columns(conn)

        event_uid = None
        deeplink_url = None
        deeplink_url_full = None
        is_fallback = False
        title = None
        channel_name = None
        synopsis = None

        event_id = None
        chosen_provider = None
        try:
            if row is not None and "chosen_provider" in row.keys():
                chosen_provider = row["chosen_provider"]
        except Exception:
            chosen_provider = None

        # Check if current event is a placeholder - if so, try fallback
        if row and row["is_placeholder"]:
            log(f"Lane {lane_id}: Current slot is placeholder, checking for fallback event within padding window", "INFO")
            fallback = get_fallback_event_for_lane(conn, lane_id, at_ts)
            
            if fallback:
                event_id = fallback['event_id']
                title = fallback['title']
                channel_name = fallback['channel_name']
                synopsis = fallback['synopsis']
                is_fallback = True
                chosen_provider = fallback.get('chosen_provider') or chosen_provider
                
                log(f"Lane {lane_id}: Using FALLBACK event '{title}' (ended at {fallback['end_utc']})", "INFO")
                
                # Get deeplink info for fallback event
                link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col)
                event_uid = link_info.get("event_uid")
                deeplink_url = link_info.get("deeplink_url")
                deeplink_url_full = link_info.get("deeplink_url_full")
            else:
                log(f"Lane {lane_id}: No fallback event found within padding window", "INFO")
        elif row and not row["is_placeholder"]:
            # Normal case: non-placeholder event
            event_id = row["event_id"]
            title = row["title"]
            channel_name = row["channel_name"]
            synopsis = row["synopsis"]
            
            link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col)
            event_uid = link_info.get("event_uid")
            deeplink_url = link_info.get("deeplink_url")
            deeplink_url_full = link_info.get("deeplink_url_full")
        
        # Convert to HTTP format if requested (for Android/Fire TV)
        if deeplink_format == "http":
            try:
                from deeplink_converter import generate_http_deeplink
                
                # Try to convert primary deeplink
                if deeplink_url:
                    playable_uuid = get_playable_id_for_event(conn, event_id, chosen_provider)
                    try:
                        http_version = generate_http_deeplink(
                            deeplink_url,
                            provider=chosen_provider,
                            playable_id=playable_uuid,
                        )
                    except TypeError:
                        # Backwards compatible with older converter signatures
                        http_version = (
                            generate_http_deeplink(deeplink_url, chosen_provider)
                            if chosen_provider
                            else generate_http_deeplink(deeplink_url)
                        )
                    if http_version:
                        deeplink_url = http_version

                # Try to convert full deeplink
                if deeplink_url_full:
                    playable_uuid = get_playable_id_for_event(conn, event_id, chosen_provider)
                    try:
                        http_version = generate_http_deeplink(
                            deeplink_url_full,
                            provider=chosen_provider,
                            playable_id=playable_uuid,
                        )
                    except TypeError:
                        http_version = (
                            generate_http_deeplink(deeplink_url_full, chosen_provider)
                            if chosen_provider
                            else generate_http_deeplink(deeplink_url_full)
                        )
                    if http_version:
                        deeplink_url_full = http_version

            except ImportError:
                log("deeplink_converter not available, using original scheme URLs", "WARN")

        conn.close()

        # Plain text mode
        if fmt == "txt":
            value = ""
            if param == "event_uid":
                value = event_uid or ""
            elif param == "deeplink_url_full":
                value = (deeplink_url_full or deeplink_url or "") or ""
            else:  # default "deeplink_url"
                value = (deeplink_url or deeplink_url_full or "") or ""

            return Response(value, mimetype="text/plain")

        # JSON mode
        payload = {
            "ok": True,
            "lane": lane_id,
            "event_uid": event_uid,
            "at": at_ts,
        }
        
        # Add title if available
        if title:
            payload["title"] = title

        if want_deeplink:
            payload["deeplink_url"] = deeplink_url
            payload["deeplink_url_full"] = deeplink_url_full
            
        # Add fallback indicator
        if is_fallback:
            payload["is_fallback"] = True

        return jsonify(payload)

    except Exception as e:
        log(f"/whatson/{lane_id} error: {e}", "ERROR")
        if fmt == "txt":
            return Response("", mimetype="text/plain")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/whatson/all")
def whatson_all():
    """Get status across all lanes at once (JSON only)."""
    if not DB_PATH.exists():
        return jsonify({"ok": False, "error": "Database not found"}), 500

    from datetime import datetime as _dt

    at_ts = request.args.get("at")
    if not at_ts:
        at_ts = _dt.utcnow().isoformat(timespec="seconds")

    want_deeplink = False
    include_param = request.args.get("include")
    if include_param == "deeplink":
        want_deeplink = True
    if request.args.get("deeplink") in ("1", "true", "yes"):
        want_deeplink = True
    if request.args.get("dynamic") in ("1", "true", "yes"):
        want_deeplink = True

    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        current_by_lane = get_current_events_by_lane(conn, at_ts=at_ts)

        uid_col, primary_col, full_col = get_event_link_columns(conn)

        items = []
        for lane_id, row in sorted(current_by_lane.items(), key=lambda kv: kv[0]):
            event_id = row.get("event_id")
            event_uid = None
            deeplink_url = None
            deeplink_url_full = None
            if event_id is not None:
                link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col)
                event_uid = link_info.get("event_uid")
                deeplink_url = link_info.get("deeplink_url")
                deeplink_url_full = link_info.get("deeplink_url_full")

            item = {
                "lane": lane_id,
                "event_uid": event_uid,
            }
            if want_deeplink:
                item["deeplink_url"] = deeplink_url
                item["deeplink_url_full"] = deeplink_url_full
            items.append(item)

        conn.close()

        return jsonify({"ok": True, "at": at_ts, "items": items})

    except Exception as e:
        log(f"/whatson/all error: {e}", "ERROR")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/lanes/<int:lane_id>/schedule")
def lane_schedule(lane_id):
    """Get current and upcoming schedule for a lane"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        now = datetime.now().isoformat()

        cur.execute(
            """
            SELECT le.*, e.title, e.channel_name, e.synopsis
            FROM lane_events le
            LEFT JOIN events e ON le.event_id = e.id
            WHERE le.lane_id = ?
              AND le.end_utc >= ?
            ORDER BY le.start_utc
            LIMIT 10
        """,
            (lane_id, now),
        )

        schedule = [dict(row) for row in cur.fetchall()]
        conn.close()

        return jsonify({"lane_id": lane_id, "schedule": schedule})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==================== Health Check ====================
@app.route("/health")
def health():
    """Health check endpoint"""
    db_ok = DB_PATH.exists()
    return jsonify(
        {
            "status": "healthy" if db_ok else "degraded",
            "database": "ok" if db_ok else "missing",
            "timestamp": datetime.now().isoformat(),
        }
    )


# ==================== HTML Templates ====================

# ==================== Template Loading ====================
def load_template(template_name):
    """Load an HTML template from the templates directory.
    
    Looks for templates in these locations (in order):
    1. /app/templates/ (Docker environment)
    2. ./templates/ (relative to script)
    3. ../templates/ (project root)
    """
    template_paths = [
        Path("/app/templates") / template_name,
        Path(__file__).parent / "templates" / template_name,
        Path(__file__).parent.parent / "templates" / template_name,
    ]
    
    for template_path in template_paths:
        if template_path.exists():
            try:
                return template_path.read_text(encoding="utf-8")
            except Exception as e:
                log(f"Error loading template {template_name}: {e}", "ERROR")
                continue
    
    # Fallback: return a minimal error page
    return f"""
    <!DOCTYPE html>
    <html>
    <head><title>Template Error</title><style>body{{font-family:sans-serif;padding:40px;background:#1a1a1a;color:#eee;}}</style></head>
    <body>
        <h1>âŒ Template Error</h1>
        <p>Could not load template: <code>{template_name}</code></p>
        <p>Searched in:</p>
        <ul>{''.join(f'<li><code>{p}</code></li>' for p in template_paths)}</ul>
        <p><a href="/">â† Back to Dashboard</a></p>
    </body>
    </html>
    """


# ==================== CDVR Detector Routes ====================
@app.route('/lane/<int:lane_number>/stream.m3u8', methods=['GET', 'HEAD'])
def serve_lane_hls(lane_number):
    """Serve a minimal *live-ish* HLS playlist and trigger auto-detection in background."""
    if not DETECTOR_ENABLED:
        return "CDVR Detector not enabled. Set CDVR_DVR_PATH in .env", 503
    
    remote = request.remote_addr
    ua = request.headers.get("User-Agent", "-")
    self_base_url = request.host_url.rstrip("/")  # e.g. http://192.168.86.80:6655

    log(f"LANE_HIT lane={lane_number} remote={remote} ua={ua}", "INFO")

    # Debounce detector spawns per-lane (CDVR often re-requests the playlist quickly)
    spawn = True
    now = time.time()
    try:
        with DETECT_LAST_SPAWN_LOCK:
            last = DETECT_LAST_SPAWN.get(int(lane_number), 0)
            if (now - float(last)) < float(DETECT_DEBOUNCE_SECONDS):
                spawn = False
            else:
                DETECT_LAST_SPAWN[int(lane_number)] = now
    except Exception:
        spawn = True

    def delayed_detect(lane: int, hint_ip: str, base_url: str):
        time.sleep(2)
        log(f"DETECTOR_START lane={lane}", "INFO")
        try:
            auto_detect_and_trigger(lane, hint_ip, base_url)
        except Exception as e:
            log(f"DETECTOR_CRASH lane={lane}: {e}", "ERROR")

    if spawn:
        threading.Thread(
            target=delayed_detect,
            args=(int(lane_number), remote, self_base_url),
            daemon=True,
        ).start()

    # Build a "live-ish" playlist:
    #  - NO #EXT-X-ENDLIST
    #  - moving MEDIA-SEQUENCE
    #  - multiple segments (same underlying TS is fine for a dummy stream)
    epoch = int(now)
    seq = epoch  # monotonic enough per request for dummy streaming

    playlist = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        "#EXT-X-TARGETDURATION:60\n"
        f"#EXT-X-MEDIA-SEQUENCE:{seq}\n"
        "#EXTINF:60.0,\n"
        f"/lane/{lane_number}/segment.ts?seq={seq}\n"
        "#EXTINF:60.0,\n"
        f"/lane/{lane_number}/segment.ts?seq={seq+1}\n"
        "#EXTINF:60.0,\n"
        f"/lane/{lane_number}/segment.ts?seq={seq+2}\n"
    )

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    if request.method == "HEAD":
        return Response("", mimetype="application/vnd.apple.mpegurl", headers=headers)

    return Response(playlist, mimetype="application/vnd.apple.mpegurl", headers=headers)

@app.route('/lane/<int:lane_number>/segment.ts')
def serve_segment(lane_number):
    """Serve the dummy video segment."""
    if DUMMY_SEGMENT_PATH and os.path.exists(DUMMY_SEGMENT_PATH):
        resp = send_file(DUMMY_SEGMENT_PATH, mimetype='video/mp2t')
        # Encourage clients to refetch even if they hit the same URL repeatedly.
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    return "Segment not available", 404


# ==================== Main ====================
if __name__ == "__main__":
    log("FruitDeepLinks server starting...", "INFO")

    port = int(os.getenv("PORT", 6655))
    host = os.getenv("HOST", "0.0.0.0")

    log(f"Server running on http://{host}:{port}", "INFO")
    log(f"Admin dashboard: http://{host}:{port}/", "INFO")
    
    # Bootstrap CDVR detector (if enabled)
    if DETECTOR_ENABLED:
        log("Initializing CDVR Detector...", "INFO")
        log(f"CDVR Server: {CDVR_SERVER_IP}:{CDVR_SERVER_PORT}", "INFO")
        log(f"Streamlink Directory: {STREAMLINK_DIR}", "INFO")
        log(f"Number of Lanes: {NUM_LANES}", "INFO")
        
        create_dummy_segment()
        bootstrap_streamlink_files()
    else:
        log("CDVR Detector: Disabled (set CDVR_DVR_PATH in .env to enable)", "INFO")

    # Start APScheduler-based auto-refresh if available
    start_scheduler_if_available()

    try:
        app.run(host=host, port=port, debug=False, threaded=True)
    finally:
        if scheduler:
            try:
                scheduler.shutdown(wait=False)
            except Exception:
                pass
