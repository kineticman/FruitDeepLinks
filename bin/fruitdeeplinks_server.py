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
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}

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
        return {
            "enabled_services": prefs.get("enabled_services", []),
            "disabled_sports": prefs.get("disabled_sports", []),
            "disabled_leagues": prefs.get("disabled_leagues", []),
        }
    except Exception as e:
        log(f"Error loading preferences: {e}", "ERROR")
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}


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


# ==================== File Serving / API ====================
@app.route("/")
def index():
    """Admin dashboard"""
    return load_template("admin_dashboard.html")



@app.route("/api")
def api_helper():
    """Simple HTML API helper page"""
    return load_template("api_helper.html")

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

            # Augment with logical web services (e.g., max, peacock_web) so they
            # can appear in the ADB Lane Providers table even though their raw
            # provider is "https".
            try:
                if LOGICAL_SERVICES_AVAILABLE:
                    service_counts = get_all_logical_services_with_counts(conn)
                    existing_codes = {row["provider_code"] for row in rows}
                    extra_rows = []
                    for service_code, _count in service_counts.items():
                        if service_code in existing_codes:
                            continue
                        # Skip generic buckets; those are either already present
                        # or not useful for user-facing ADB config.
                        if service_code in ("http", "https"):
                            continue
                        extra_rows.append(
                            {
                                "provider_code": service_code,
                                "adb_enabled": 0,
                                "adb_lane_count": 0,
                                "created_at": None,
                                "updated_at": None,
                            }
                        )
                    if extra_rows:
                        rows.extend(sorted(extra_rows, key=lambda r: r["provider_code"]))
            except Exception as e:
                log(f"api_provider_lanes: failed to merge logical services: {e}", "ERROR")

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
    - at: ISO timestamp (default: now)
    
    Examples:
    - /api/adb/lanes/sportscenter/1/deeplink?format=text
    - /api/adb/lanes/pplus/3/deeplink?format=json
    
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
                "event_end_utc": event_row["end_utc"]
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

    Plain text deeplink:
      GET /whatson/1?format=txt&param=deeplink_url
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

        if row:
            event_id = row["event_id"]
            link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col)
            event_uid = link_info.get("event_uid")
            deeplink_url = link_info.get("deeplink_url")
            deeplink_url_full = link_info.get("deeplink_url_full")

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

        if want_deeplink:
            payload["deeplink_url"] = deeplink_url
            payload["deeplink_url_full"] = deeplink_url_full

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
        <h1>❌ Template Error</h1>
        <p>Could not load template: <code>{template_name}</code></p>
        <p>Searched in:</p>
        <ul>{''.join(f'<li><code>{p}</code></li>' for p in template_paths)}</ul>
        <p><a href="/">← Back to Dashboard</a></p>
    </body>
    </html>
    """




# ==================== Main ====================
if __name__ == "__main__":
    log("FruitDeepLinks server starting...", "INFO")

    port = int(os.getenv("PORT", 6655))
    host = os.getenv("HOST", "0.0.0.0")

    log(f"Server running on http://{host}:{port}", "INFO")
    log(f"Admin dashboard: http://{host}:{port}/", "INFO")

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
