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
    render_template_string,
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
DB_PATH = Path(os.getenv("PEACOCK_DB_PATH", "/app/data/fruit_events.db"))
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
    return render_template_string(ADMIN_TEMPLATE)



@app.route("/api")
def api_helper():
    """Simple HTML API helper page"""
    return render_template_string(API_HELPER_TEMPLATE)

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
                    "peacock_build_lanes.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "peacock_build_lanes.py"),
                        "--db",
                        str(DB_PATH),
                        "--lanes",
                        os.getenv("PEACOCK_LANES", "50"),
                    ],
                ),
                (
                    "peacock_export_hybrid.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "peacock_export_hybrid.py"),
                        "--db",
                        str(DB_PATH),
                    ],
                ),
                (
                    "peacock_export_lanes.py",
                    [
                        "python3",
                        "-u",
                        str(BIN_DIR / "peacock_export_lanes.py"),
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
    return render_template_string(FILTERS_TEMPLATE)


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
FILTERS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Filters & Settings - FruitDeepLinks</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            color: #e2e8f0;
            padding: 20px;
            min-height: 100vh;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { font-size: 32px; margin-bottom: 10px; }
        .subtitle { color: #94a3b8; margin-bottom: 30px; }
        .nav {
            margin-bottom: 30px;
            display: flex;
            gap: 15px;
        }
        .nav a {
            color: #60a5fa;
            text-decoration: none;
            padding: 8px 16px;
            border-radius: 6px;
            background: #1e293b;
            border: 1px solid #334155;
        }
        .nav a:hover { background: #334155; }
        .card {
            background: #1e293b;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
            border: 1px solid #334155;
        }
        h2 {
            font-size: 20px;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .section-description {
            color: #94a3b8;
            margin-bottom: 20px;
            font-size: 14px;
        }
        .filter-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 12px;
        }
        .filter-item {
            background: #0f172a;
            border: 2px solid #334155;
            border-radius: 8px;
            padding: 12px;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        .filter-item:hover {
            border-color: #3b82f6;
            background: #1e293b;
        }
        .filter-item.enabled {
            border-color: #22c55e;
            background: #14532d;
        }
        .filter-item.disabled {
            border-color: #ef4444;
            background: #7f1d1d;
        }
        .checkbox {
            width: 20px;
            height: 20px;
            border: 2px solid #475569;
            border-radius: 4px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
        }
        .filter-item.enabled .checkbox {
            background: #22c55e;
            border-color: #22c55e;
        }
        .filter-item.disabled .checkbox {
            background: #ef4444;
            border-color: #ef4444;
        }
        .checkbox::after {
            content: '✓';
            color: white;
            font-weight: bold;
            display: none;
        }
        .filter-item.enabled .checkbox::after,
        .filter-item.disabled .checkbox::after {
            display: block;
        }
        .filter-item.disabled .checkbox::after {
            content: '✗';
        }
        .filter-info {
            flex: 1;
        }
        .filter-name {
            font-weight: 600;
            margin-bottom: 4px;
        }
        .filter-count {
            font-size: 12px;
            color: #94a3b8;
        }
        .btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            margin-right: 10px;
        }
        .btn:hover { background: #2563eb; }
        .btn:disabled { background: #475569; cursor: not-allowed; }
        .btn-success { background: #22c55e; }
        .btn-success:hover { background: #16a34a; }
        .actions {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #334155;
            display: flex;
            gap: 10px;
            align-items: center;
        }
        .status-message {
            margin-left: auto;
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 14px;
            display: none;
        }
        .status-message.success {
            background: #14532d;
            color: #22c55e;
            border: 1px solid #22c55e;
        }
        .status-message.error {
            background: #7f1d1d;
            color: #ef4444;
            border: 1px solid #ef4444;
        }
        .loading {
            text-align: center;
            padding: 40px;
            color: #94a3b8;
        }
        .stats-summary {
            display: flex;
            gap: 20px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }
        .stat-box {
            background: #0f172a;
            padding: 12px 20px;
            border-radius: 8px;
            border: 1px solid #334155;
        }
        .stat-label {
            font-size: 12px;
            color: #94a3b8;
            margin-bottom: 4px;
        }
        .stat-value {
            font-size: 24px;
            font-weight: 600;
            color: #60a5fa;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>⚙️ Filters & Settings</h1>
        <p class="subtitle">Configure which services and content types to include in your channels</p>
        
        <div class="nav">
            <a href="/">← Back to Dashboard</a>
        </div>
        
        <div class="stats-summary" id="stats-summary">
            <div class="stat-box">
                <div class="stat-label">Streaming Services</div>
                <div class="stat-value" id="stat-providers">-</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Sports Available</div>
                <div class="stat-value" id="stat-sports">-</div>
            </div>
            <div class="stat-box">
                <div class="stat-label">Leagues Available</div>
                <div class="stat-value" id="stat-leagues">-</div>
            </div>
        </div>
        
        <div class="card">
            <h2>📺 Streaming Services</h2>
            <p class="section-description">
                Select which streaming services you have subscriptions to. Only events available on your selected services will be included in generated playlists.
                <strong>Green = Enabled</strong> (will be included) | <strong>Red = Disabled</strong> (will be excluded)
            </p>
            <div id="providers-loading" class="loading">Loading services...</div>
            <div id="providers-grid" class="filter-grid" style="display: none;"></div>
        </div>
        
        <div class="card">
            <h2>🏀 Sports Filter</h2>
            <p class="section-description">
                Hide sports you're not interested in. Disabled sports will be excluded from your channels.
            </p>
            <div id="sports-loading" class="loading">Loading sports...</div>
            <div id="sports-grid" class="filter-grid" style="display: none;"></div>
        </div>
        
        <div class="card">
            <h2>🏆 Leagues Filter</h2>
            <p class="section-description">
                Hide specific leagues or competitions. Disabled leagues will be excluded from your channels.
            </p>
            <div id="leagues-loading" class="loading">Loading leagues...</div>
            <div id="leagues-grid" class="filter-grid" style="display: none;"></div>
        </div>
        
        <div class="actions">
            <button class="btn btn-success" onclick="savePreferences()">💾 Save Settings</button>
            <button class="btn" style="background: #f59e0b;" onclick="applyFilters()">🎯 Apply Filters Now</button>
            <button class="btn" onclick="resetToDefaults()">🔄 Reset to Defaults</button>
            <div class="status-message" id="status-message"></div>
        </div>
    </div>
    
    <script>
        let currentPreferences = {
            enabled_services: [],
            disabled_sports: [],
            disabled_leagues: []
        };
        
        let availableFilters = {
            providers: [],
            sports: [],
            leagues: []
        };
        
        async function loadFilters() {
            try {
                const res = await fetch('/api/filters');
                const data = await res.json();
                
                availableFilters = data.filters;
                currentPreferences = data.preferences;
                
                // If no enabled services, default to ALL enabled
                if (currentPreferences.enabled_services.length === 0) {
                    currentPreferences.enabled_services = availableFilters.providers.map(p => p.scheme);
                }
                
                renderProviders();
                renderSports();
                renderLeagues();
                updateStats();
                
            } catch (err) {
                console.error('Failed to load filters:', err);
                showStatus('Failed to load filters', 'error');
            }
        }
        
        function renderProviders() {
            const grid = document.getElementById('providers-grid');
            const loading = document.getElementById('providers-loading');
            
            if (availableFilters.providers.length === 0) {
                loading.textContent = 'No streaming services found. Run a refresh first.';
                return;
            }
            
            grid.innerHTML = availableFilters.providers.map(provider => {
                const isEnabled = currentPreferences.enabled_services.includes(provider.scheme);
                return `
                    <div class="filter-item ${isEnabled ? 'enabled' : 'disabled'}" 
                         onclick="toggleProvider('${provider.scheme}')">
                        <div class="checkbox"></div>
                        <div class="filter-info">
                            <div class="filter-name">${provider.name}</div>
                            <div class="filter-count">${provider.count} events</div>
                        </div>
                    </div>
                `;
            }).join('');
            
            loading.style.display = 'none';
            grid.style.display = 'grid';
        }
        
        function renderSports() {
            const grid = document.getElementById('sports-grid');
            const loading = document.getElementById('sports-loading');
            
            if (availableFilters.sports.length === 0) {
                loading.textContent = 'No sports data found.';
                return;
            }
            
            grid.innerHTML = availableFilters.sports.map(sport => {
                const isDisabled = currentPreferences.disabled_sports.includes(sport.name);
                return `
                    <div class="filter-item ${isDisabled ? 'disabled' : 'enabled'}" 
                         onclick="toggleSport(\`${sport.name}\`)">
                        <div class="checkbox"></div>
                        <div class="filter-info">
                            <div class="filter-name">${sport.name}</div>
                            <div class="filter-count">${sport.count} events</div>
                        </div>
                    </div>
                `;
            }).join('');
            
            loading.style.display = 'none';
            grid.style.display = 'grid';
        }
        
        function renderLeagues() {
            const grid = document.getElementById('leagues-grid');
            const loading = document.getElementById('leagues-loading');
            
            if (availableFilters.leagues.length === 0) {
                loading.textContent = 'No leagues data found.';
                return;
            }
            
            grid.innerHTML = availableFilters.leagues.map(league => {
                const isDisabled = currentPreferences.disabled_leagues.includes(league.name);
                return `
                    <div class="filter-item ${isDisabled ? 'disabled' : 'enabled'}" 
                         onclick="toggleLeague(\`${league.name}\`)">
                        <div class="checkbox"></div>
                        <div class="filter-info">
                            <div class="filter-name">${league.name}</div>
                            <div class="filter-count">${league.count} events</div>
                        </div>
                    </div>
                `;
            }).join('');
            
            loading.style.display = 'none';
            grid.style.display = 'grid';
        }
        
        function toggleProvider(scheme) {
            const index = currentPreferences.enabled_services.indexOf(scheme);
            if (index > -1) {
                currentPreferences.enabled_services.splice(index, 1);
            } else {
                currentPreferences.enabled_services.push(scheme);
            }
            renderProviders();
        }
        
        function toggleSport(name) {
            const index = currentPreferences.disabled_sports.indexOf(name);
            if (index > -1) {
                currentPreferences.disabled_sports.splice(index, 1);
            } else {
                currentPreferences.disabled_sports.push(name);
            }
            renderSports();
        }
        
        function toggleLeague(name) {
            const index = currentPreferences.disabled_leagues.indexOf(name);
            if (index > -1) {
                currentPreferences.disabled_leagues.splice(index, 1);
            } else {
                currentPreferences.disabled_leagues.push(name);
            }
            renderLeagues();
        }
        
        async function savePreferences() {
            try {
                const res = await fetch('/api/filters/preferences', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(currentPreferences)
                });
                
                if (res.ok) {
                    showStatus('✓ Settings saved! Click "Apply Filters Now" to regenerate channels.', 'success');
                } else {
                    showStatus('✗ Failed to save settings', 'error');
                }
            } catch (err) {
                showStatus('✗ Error saving settings', 'error');
            }
        }
        
        async function applyFilters() {
            // Save first, then apply
            try {
                const saveRes = await fetch('/api/filters/preferences', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(currentPreferences)
                });
                
                if (!saveRes.ok) {
                    showStatus('✗ Failed to save settings', 'error');
                    return;
                }
                
                // Now apply filters
                const applyRes = await fetch('/api/apply-filters', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'}
                });
                
                if (applyRes.ok) {
                    showStatus('🎯 Applying filters... This takes ~10 seconds. Check dashboard for progress.', 'success');
                    
                    // Redirect to dashboard after 2 seconds
                    setTimeout(() => {
                        window.location.href = '/';
                    }, 2000);
                } else {
                    const data = await applyRes.json();
                    showStatus('✗ ' + (data.error || 'Failed to apply filters'), 'error');
                }
            } catch (err) {
                showStatus('✗ Error applying filters', 'error');
            }
        }
        
        function resetToDefaults() {
            if (confirm('Reset all filters to defaults? This will enable all services and sports.')) {
                currentPreferences = {
                    enabled_services: availableFilters.providers.map(p => p.scheme),
                    disabled_sports: [],
                    disabled_leagues: []
                };
                renderProviders();
                renderSports();
                renderLeagues();
                showStatus('Reset to defaults. Click Save to apply.', 'success');
            }
        }
        
        function showStatus(message, type) {
            const statusEl = document.getElementById('status-message');
            statusEl.textContent = message;
            statusEl.className = `status-message ${type}`;
            statusEl.style.display = 'block';
            setTimeout(() => {
                statusEl.style.display = 'none';
            }, 5000);
        }
        
        function updateStats() {
            document.getElementById('stat-providers').textContent = availableFilters.providers.length;
            document.getElementById('stat-sports').textContent = availableFilters.sports.length;
            document.getElementById('stat-leagues').textContent = availableFilters.leagues.length;
        }
        
        // Initialize
        loadFilters();
    </script>
</body>
</html>
"""

ADMIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>FruitDeepLinks Admin</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color: #60a5fa; margin-bottom: 10px; }
        .subtitle { color: #94a3b8; margin-bottom: 30px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card {
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 20px;
        }
        .card h2 { color: #60a5fa; font-size: 18px; margin-bottom: 15px; }
        .stat { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #334155; }
        .stat:last-child { border-bottom: none; }
        .stat-label { color: #94a3b8; }
        .stat-value { color: #e2e8f0; font-weight: 600; }
        .btn {
            background: #3b82f6;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
            margin-right: 10px;
        }
        .btn:hover { background: #2563eb; }
        .btn:disabled { background: #475569; cursor: not-allowed; }
        .btn-secondary { background: #64748b; }
        .btn-secondary:hover { background: #475569; }
        .log-container {
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 15px;
            height: 400px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 13px;
        }
        .log-line { padding: 2px 0; color: #cbd5e1; }
        .log-line:hover { background: #1e293b; }
        .status-running { color: #fbbf24; }
        .status-success { color: #34d399; }
        .status-failed { color: #f87171; }
        .file-list { list-style: none; }
        .file-item { padding: 8px 0; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; }
        .file-item:last-child { border-bottom: none; }
        .file-name { color: #60a5fa; }
        .file-size { color: #94a3b8; font-size: 12px; }
        .loading { color: #fbbf24; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🍎 FruitDeepLinks Admin</h1>
        <p class="subtitle">Multi-source sports event aggregator</p>
        
        <div style="margin-bottom: 20px;">
            <a href="/filters" style="display: inline-block; padding: 10px 20px; background: #3b82f6; color: white; text-decoration: none; border-radius: 6px; font-weight: 600;">⚙️ Filters & Settings</a>
            <a href="/api" style="display: inline-block; padding: 10px 20px; background: #0f172a; border: 1px solid #1f2937; border-radius: 6px; font-weight: 600; margin-left: 8px;">📚 API Helper</a>
        </div>
        
        <div class="grid">
            <div class="card">
                <h2>📊 Database Stats</h2>
                <div id="db-stats">Loading...</div>
            </div>
            
            <div class="card">
                <h2>📁 Output Files</h2>
                <div id="file-list">Loading...</div>
            </div>
            
            <div class="card">
                <h2>🔄 Refresh Control</h2>
                <div id="refresh-status" style="margin-bottom: 15px;">Loading...</div>
                <button class="btn" onclick="triggerRefresh(false)" id="btn-refresh">Full Refresh</button>
                <button class="btn btn-secondary" onclick="triggerRefresh(true)" id="btn-refresh-skip">Skip Scrape</button>

                <div style="margin-top:15px; padding-top:15px; border-top:1px solid #334155;">
                    <h3 style="font-size:15px; margin-bottom:8px;">⏰ Auto Refresh</h3>
                    <div style="margin-bottom:8px;">
                        <label class="stat-label">
                            Time (local):
                            <input type="time" id="auto-time" style="margin-left:8px; padding:4px; border-radius:4px; border:1px solid #334155; background:#0f172a; color:#e2e8f0;">
                        </label>
                    </div>
                    <div style="margin-bottom:8px;">
                        <label class="stat-label">
                            <input type="checkbox" id="auto-enabled" style="margin-right:6px;">
                            Enable daily auto refresh
                        </label>
                    </div>
                    <div class="stat-label" id="auto-next-run"></div>
                    <div style="margin-top:10px;">
                        <button class="btn btn-secondary" onclick="saveAutoRefresh()" id="btn-save-auto">Save Auto Settings</button>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h2>📝 Live Logs <span class="loading" id="log-status"></span></h2>
            <div class="log-container" id="log-container"></div>
        </div>
    </div>
    
    <script>
        let logEventSource;
        
        async function loadStatus() {
            try {
                const res = await fetch('/api/status');
                if (!res.ok) {
                    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
                }
                const data = await res.json();
                
                // Database stats
                const dbStats = document.getElementById('db-stats');
                if (data.database.error) {
                    dbStats.innerHTML = `<div class="stat-value" style="color: #f87171;">Error: ${data.database.error}</div>`;
                } else {
                    const db = data.database;
                    const dbMod = db.db_modified ? new Date(db.db_modified).toLocaleString() : 'n/a';
                    const dbSize = db.db_size ? (db.db_size / 1024 / 1024).toFixed(2) + ' MB' : 'n/a';
                    dbStats.innerHTML = `
                        <div class="stat"><span class="stat-label">Total Events</span><span class="stat-value">${db.total_events || 0}</span></div>
                        <div class="stat"><span class="stat-label">Future Events</span><span class="stat-value">${db.future_events || 0}</span></div>
                        <div class="stat"><span class="stat-label">Lanes</span><span class="stat-value">${db.lane_count || 0}</span></div>
                        <div class="stat"><span class="stat-label">Scheduled</span><span class="stat-value">${db.scheduled_events || 0}</span></div>
                        <div class="stat"><span class="stat-label">DB Last Updated</span><span class="stat-value">${dbMod}</span></div>
                        <div class="stat"><span class="stat-label">DB Size</span><span class="stat-value">${dbSize}</span></div>
                    `;
                }
                
                // Files
                const fileList = document.getElementById('file-list');
                const files = Object.entries(data.files || {});
                if (files.length === 0) {
                    fileList.innerHTML = '<p class="stat-value">No files generated yet</p>';
                } else {
                    fileList.innerHTML = '<ul class="file-list">' + files.map(([name, info]) => `
                        <li class="file-item">
                            <a href="/out/${name}" class="file-name">${name}</a>
                            <span class="file-size">${(info.size / 1024 / 1024).toFixed(2)} MB</span>
                        </li>
                    `).join('') + '</ul>';
                }
                
                // Refresh status
                const refreshStatus = document.getElementById('refresh-status');
                const refresh = data.refresh;
                const btnRefresh = document.getElementById('btn-refresh');
                const btnRefreshSkip = document.getElementById('btn-refresh-skip');
                
                if (refresh.running) {
                    refreshStatus.innerHTML = `<span class="status-running">⚙️ Running: ${refresh.current_step || 'Processing...'}</span>`;
                    btnRefresh.disabled = true;
                    btnRefreshSkip.disabled = true;
                } else {
                    btnRefresh.disabled = false;
                    btnRefreshSkip.disabled = false;
                    
                    const pieces = [];
                    if (refresh.last_run_manual) {
                        const manualStatus = refresh.last_status_manual || refresh.last_status;
                        let manualBadge = '';
                        if (manualStatus === 'success') {
                            manualBadge = '<span class="status-success">✓ Success</span>';
                        } else if (manualStatus) {
                            manualBadge = '<span class="status-failed">✗ ' + manualStatus + '</span>';
                        }
                        pieces.push('Last manual refresh: ' + new Date(refresh.last_run_manual).toLocaleString() + ' ' + manualBadge);
                    }
                    if (refresh.last_run_auto) {
                        const autoStatus = refresh.last_status_auto;
                        let autoBadge = '';
                        if (autoStatus === 'success') {
                            autoBadge = '<span class="status-success">✓ Success</span>';
                        } else if (autoStatus) {
                            autoBadge = '<span class="status-failed">✗ ' + autoStatus + '</span>';
                        }
                        pieces.push('Last auto refresh: ' + new Date(refresh.last_run_auto).toLocaleString() + ' ' + autoBadge);
                    }
                    if (!pieces.length && refresh.last_run) {
                        const status = refresh.last_status === 'success' 
                            ? '<span class="status-success">✓ Success</span>'
                            : refresh.last_status
                                ? '<span class="status-failed">✗ ' + refresh.last_status + '</span>'
                                : '';
                        pieces.push('Last refresh: ' + new Date(refresh.last_run).toLocaleString() + ' ' + status);
                    }
                    refreshStatus.innerHTML = pieces.length ? pieces.join('<br>') : 'No refresh run yet';
                }

                // Auto-refresh status block
                if (data.auto_refresh) {
                    applyAutoSettingsToDom(data.auto_refresh);
                }
                
            } catch (err) {
                console.error('Failed to load status:', err);
                document.getElementById('db-stats').innerHTML = `<div style="color: #f87171;">API Error: ${err.message}</div>`;
                document.getElementById('file-list').innerHTML = `<div style="color: #f87171;">Failed to load files</div>`;
                document.getElementById('refresh-status').innerHTML = `<div style="color: #f87171;">Status unavailable</div>`;
            }
        }
        
        async function triggerRefresh(skipScrape) {
            try {
                const res = await fetch('/api/refresh', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({skip_scrape: skipScrape})
                });
                
                if (res.ok) {
                    document.getElementById('log-status').textContent = '🔴 LIVE';
                    loadStatus();
                } else {
                    const data = await res.json();
                    alert(data.error || 'Failed to start refresh');
                }
            } catch (err) {
                alert('Error: ' + err.message);
            }
        }

        async function saveAutoRefresh() {
            try {
                const enabled = document.getElementById('auto-enabled').checked;
                const time = document.getElementById('auto-time').value || '02:30';
                const res = await fetch('/api/auto-refresh', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ enabled, time })
                });
                if (!res.ok) {
                    const data = await res.json().catch(() => ({}));
                    alert(data.error || 'Failed to save auto-refresh settings');
                    return;
                }
                const dataJson = await res.json();
                applyAutoSettingsToDom(dataJson);
            } catch (err) {
                alert('Error saving auto refresh: ' + err.message);
            }
        }

        function applyAutoSettingsToDom(auto) {
            const timeInput = document.getElementById('auto-time');
            const enabledInput = document.getElementById('auto-enabled');
            const label = document.getElementById('auto-next-run');

            if (timeInput && document.activeElement !== timeInput) {
                timeInput.value = auto.time || '02:30';
            }
            if (enabledInput && document.activeElement !== enabledInput) {
                enabledInput.checked = !!auto.enabled;
            }

            if (auto.enabled) {
                if (auto.next_run) {
                    label.textContent = 'Next auto run: ' + new Date(auto.next_run).toLocaleString();
                } else {
                    label.textContent = 'Next auto run: scheduling...';
                }
            } else {
                label.textContent = 'Auto refresh is disabled';
            }
        }
        
        function setupLogStream() {
            if (logEventSource) logEventSource.close();
            
            logEventSource = new EventSource('/api/logs/stream');
            const container = document.getElementById('log-container');
            
            logEventSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                const line = document.createElement('div');
                line.className = 'log-line';
                line.textContent = data.log;
                container.appendChild(line);
                container.scrollTop = container.scrollHeight;
                
                // Keep only last 500 lines
                while (container.children.length > 500) {
                    container.removeChild(container.firstChild);
                }
            };
            
            logEventSource.onerror = () => {
                document.getElementById('log-status').textContent = '⚫ Reconnecting...';
                setTimeout(setupLogStream, 5000);
            };
            
            document.getElementById('log-status').textContent = '🔴 LIVE';
        }
        
        async function loadInitialLogs() {
            const res = await fetch('/api/logs?count=100');
            const data = await res.json();
            const container = document.getElementById('log-container');
            container.innerHTML = data.logs.map(log => 
                `<div class="log-line">${log}</div>`
            ).join('');
            container.scrollTop = container.scrollHeight;
        }
        
        // Initialize
        loadStatus();
        loadInitialLogs();
        setupLogStream();
        setInterval(loadStatus, 5000);
    </script>
</body>
</html>
"""
API_HELPER_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
    <title>FruitDeepLinks API Helper</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #020617;
            color: #e2e8f0;
            padding: 20px;
        }
        a { color: #38bdf8; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { color: #60a5fa; margin-bottom: 4px; }
        .subtitle { color: #94a3b8; margin-bottom: 20px; }
        .card {
            background: #020617;
            border: 1px solid #1f2937;
            border-radius: 8px;
            padding: 16px 20px;
            margin-bottom: 16px;
        }
        .card h2 { font-size: 18px; margin-bottom: 8px; color: #f97316; }
        code {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
            font-size: 13px;
            background: #020617;
            padding: 2px 4px;
            border-radius: 4px;
        }
        pre {
            background: #020617;
            padding: 8px 10px;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 13px;
            border: 1px solid #1f2937;
        }
        .tag {
            display: inline-block;
            font-size: 11px;
            padding: 2px 6px;
            border-radius: 4px;
            margin-right: 6px;
            margin-bottom: 4px;
            background: #111827;
            border: 1px solid #1f2937;
            color: #a5b4fc;
        }
        .small { font-size: 13px; color: #9ca3af; }
        .back-link { margin-bottom: 16px; display: inline-block; }
    </style>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">⬅ Back to Admin Dashboard</a>
        <h1>🍎 FruitDeepLinks API Helper</h1>
        <p class="subtitle">Quick reference for common HTTP endpoints.</p>

        <div class="card">
            <h2>Health &amp; Status</h2>
            <div class="small">
                <span class="tag">GET</span><code>/health</code> &mdash; basic health probe<br>
                <span class="tag">GET</span><code>/api/status</code> &mdash; detailed DB + output status
            </div>
            <pre>curl "$BASE/health"
curl "$BASE/api/status"</pre>
        </div>

        <div class="card">
            <h2>XMLTV &amp; M3U</h2>
            <div class="small">
                <span class="tag">GET</span><code>/xmltv/direct</code><br>
                <span class="tag">GET</span><code>/m3u/direct</code><br>
                <span class="tag">GET</span><code>/xmltv/lanes</code><br>
                <span class="tag">GET</span><code>/m3u/lanes</code>
            </div>
            <pre>curl -o direct.xml "$BASE/xmltv/direct"
curl -o direct.m3u "$BASE/m3u/direct"</pre>
        </div>

        <div class="card">
            <h2>Lane What&apos;s On</h2>
            <div class="small">
                <span class="tag">GET</span><code>/whatson/&lt;lane&gt;</code> (JSON)<br>
                <span class="tag">GET</span><code>/whatson/&lt;lane&gt;?include=deeplink</code><br>
                <span class="tag">GET</span><code>/whatson/&lt;lane&gt;?format=txt&amp;param=deeplink_url</code>
            </div>
            <pre># JSON: lane 1, include deeplink fields
curl "$BASE/whatson/1?include=deeplink"

# Plain text: best deeplink for lane 1
curl "$BASE/whatson/1?format=txt&amp;param=deeplink_url"</pre>
        </div>

        <div class="card">
            <h2>Filters</h2>
            <div class="small">
                <span class="tag">GET</span><code>/api/filters</code> &mdash; available values + preferences<br>
                <span class="tag">GET</span><code>/api/filters/preferences</code><br>
                <span class="tag">POST</span><code>/api/filters/preferences</code>
            </div>
            <pre># Inspect current preferences
curl "$BASE/api/filters/preferences"

# Update enabled services (example)
curl -X POST "$BASE/api/filters/preferences" \
  -H "Content-Type: application/json" \
  -d '{"enabled_services":["sportscenter","peacock_web"]}'</pre>
        </div>

        <div class="card">
            <h2>Logs</h2>
            <div class="small">
                <span class="tag">GET</span><code>/api/logs?count=100</code><br>
                <span class="tag">GET</span><code>/api/logs/stream</code> (SSE)
            </div>
            <pre>curl "$BASE/api/logs?count=100"</pre>
        </div>

        <p class="small">Tip: set <code>BASE=http://HOST:6655</code> in your shell to copy/paste these examples.</p>
    </div>
</body>
</html>"""


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
