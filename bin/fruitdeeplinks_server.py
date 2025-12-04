#!/usr/bin/env python3
"""
fruitdeeplinks_server.py - Web server for FruitDeepLinks
Features: Admin panel, live logging, stream proxying, filtering (future)
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
from flask import Flask, render_template_string, jsonify, request, send_file, Response, stream_with_context
from flask_cors import CORS

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
        get_service_display_name as get_logical_service_display_name
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
    "current_step": None
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

# ==================== Database Queries ====================
def get_db_connection():
    """Get database connection"""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(str(DB_PATH))

def get_user_preferences():
    """Get user filtering preferences"""
    conn = get_db_connection()
    if not conn:
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}
    
    try:
        cur = conn.cursor()
        # Check if user_preferences table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'")
        if not cur.fetchone():
            return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}
        
        prefs = {}
        cur.execute("SELECT key, value FROM user_preferences")
        for row in cur.fetchall():
            key, value = row
            try:
                prefs[key] = json.loads(value) if value else []
            except:
                prefs[key] = []
        
        conn.close()
        return {
            "enabled_services": prefs.get("enabled_services", []),
            "disabled_sports": prefs.get("disabled_sports", []),
            "disabled_leagues": prefs.get("disabled_leagues", [])
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
        now = datetime.now().isoformat()
        
        for key, value in prefs.items():
            cur.execute(
                "INSERT OR REPLACE INTO user_preferences (key, value, updated_utc) VALUES (?, ?, ?)",
                (key, json.dumps(value), now)
            )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log(f"Error saving preferences: {e}", "ERROR")
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
                
                for service_code, count in sorted(service_counts.items(), key=lambda x: -x[1]):
                    display_name = get_logical_service_display_name(service_code)
                    providers.append({
                        "scheme": service_code,
                        "name": display_name,
                        "count": count
                    })
            else:
                # Fallback: use raw provider grouping
                cur.execute("""
                    SELECT provider, COUNT(*) as count
                    FROM playables
                    WHERE provider IS NOT NULL AND provider != ''
                    GROUP BY provider
                    ORDER BY count DESC
                """)
                for row in cur.fetchall():
                    provider, count = row
                    display_name = get_provider_display_name(provider)
                    providers.append({
                        "scheme": provider,
                        "name": display_name,
                        "count": count
                    })
        except Exception as e:
            log(f"Error loading providers: {e}", "ERROR")
        
        # Get sports from genres_json - simpler approach
        sports = {}
        cur.execute("""
            SELECT genres_json, COUNT(*) as event_count
            FROM events 
            WHERE end_utc > datetime('now')
            AND genres_json IS NOT NULL 
            AND genres_json != '[]'
            GROUP BY genres_json
        """)
        for row in cur.fetchall():
            genres_json, event_count = row
            try:
                genres = json.loads(genres_json)
                for genre in genres:
                    if genre and isinstance(genre, str):
                        sports[genre] = sports.get(genre, 0) + event_count
            except:
                pass
        
        sports_list = [{"name": k, "count": v} for k, v in sorted(sports.items(), key=lambda x: -x[1])]
        
        # Get leagues from classification_json
        leagues = {}
        cur.execute("""
            SELECT classification_json, COUNT(*) as event_count
            FROM events
            WHERE end_utc > datetime('now')
            AND classification_json IS NOT NULL
            AND classification_json != '[]'
            GROUP BY classification_json
        """)
        for row in cur.fetchall():
            class_json, event_count = row
            try:
                classifications = json.loads(class_json)
                for item in classifications:
                    if isinstance(item, dict) and item.get('type') == 'league':
                        league_name = item.get('value')
                        if league_name:
                            leagues[league_name] = leagues.get(league_name, 0) + event_count
            except:
                pass
        
        leagues_list = [{"name": k, "count": v} for k, v in sorted(leagues.items(), key=lambda x: -x[1])[:50]]
        
        conn.close()
        return {
            "providers": providers,
            "sports": sports_list,
            "leagues": leagues_list
        }
    except Exception as e:
        log(f"Error getting filters: {e}", "ERROR")
        return {"providers": [], "sports": [], "leagues": []}

def get_db_stats():
    """Get database statistics"""
    if not DB_PATH.exists():
        return {"error": "Database not found"}
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        
        stats = {}
        
        # Total events
        cur.execute("SELECT COUNT(*) FROM events")
        stats["total_events"] = cur.fetchone()[0]
        
        # Future events
        cur.execute("SELECT COUNT(*) FROM events WHERE end_utc > datetime('now')")
        stats["future_events"] = cur.fetchone()[0]
        
        # Events by provider
        cur.execute("""
            SELECT channel_name, COUNT(*) as count 
            FROM events 
            WHERE end_utc > datetime('now')
            GROUP BY channel_name 
            ORDER BY count DESC 
            LIMIT 10
        """)
        stats["top_providers"] = [
            {"name": row[0], "count": row[1]} 
            for row in cur.fetchall()
        ]
        
        # Lane statistics
        try:
            cur.execute("SELECT COUNT(*) FROM lanes")
            stats["lane_count"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM lane_events WHERE is_placeholder = 0")
            stats["scheduled_events"] = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM lane_events WHERE is_placeholder = 1")
            stats["placeholders"] = cur.fetchone()[0]
        except:
            stats["lane_count"] = 0
            stats["scheduled_events"] = 0
            stats["placeholders"] = 0
        
        conn.close()
        return stats
        
    except Exception as e:
        return {"error": str(e)}

# ==================== File Serving ====================
@app.route("/")
def index():
    """Admin dashboard"""
    return render_template_string(ADMIN_TEMPLATE)

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
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
    
    return jsonify({
        "status": "online",
        "database": stats,
        "files": files,
        "refresh": refresh_status,
        "timestamp": datetime.now().isoformat()
    })

@app.route("/api/logs")
def api_logs():
    """Get recent logs"""
    count = request.args.get("count", 100, type=int)
    return jsonify({
        "logs": list(log_buffer)[-count:],
        "count": len(log_buffer)
    })

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
    
    def run_refresh():
        refresh_status["running"] = True
        refresh_status["current_step"] = "Starting refresh..."
        log("Manual refresh triggered", "INFO")
        
        try:
            cmd = ["python3", "-u", str(BIN_DIR / "daily_refresh.py")]
            if skip_scrape:
                cmd.append("--skip-scrape")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            for line in process.stdout:
                log_buffer.append(line.strip())
                if "[" in line and "/" in line and "]" in line:
                    refresh_status["current_step"] = line.strip()
            
            process.wait()
            
            if process.returncode == 0:
                refresh_status["last_status"] = "success"
                log("Refresh completed successfully", "INFO")
            else:
                refresh_status["last_status"] = "failed"
                log(f"Refresh failed with code {process.returncode}", "ERROR")
                
        except Exception as e:
            refresh_status["last_status"] = "error"
            log(f"Refresh error: {str(e)}", "ERROR")
        
        finally:
            refresh_status["running"] = False
            refresh_status["last_run"] = datetime.now().isoformat()
    
    thread = threading.Thread(target=run_refresh, daemon=True)
    thread.start()
    return jsonify({"status": "started"})

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
                ("peacock_build_lanes.py", ["python3", "-u", str(BIN_DIR / "peacock_build_lanes.py"), "--db", str(DB_PATH), "--lanes", os.getenv("PEACOCK_LANES", "50")]),
                ("peacock_export_hybrid.py", ["python3", "-u", str(BIN_DIR / "peacock_export_hybrid.py"), "--db", str(DB_PATH)]),
                ("peacock_export_lanes.py", ["python3", "-u", str(BIN_DIR / "peacock_export_lanes.py"), "--db", str(DB_PATH), "--server-url", os.getenv("SERVER_URL", "http://192.168.86.80:6655")])
            ]
            
            for script_name, cmd in scripts:
                refresh_status["current_step"] = f"Running {script_name}..."
                log(f"Running {script_name}", "INFO")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
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
            refresh_status["last_run"] = datetime.now().isoformat()
    
    thread = threading.Thread(target=run_apply_filters, daemon=True)
    thread.start()
    return jsonify({"status": "started"})

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

@app.route("/filters")
def filters_page():
    """Filters configuration page"""
    return render_template_string(FILTERS_TEMPLATE)

@app.route("/api/filters")
def api_filters():
    """Get available filters (providers, sports, leagues)"""
    filters = get_available_filters()
    prefs = get_user_preferences()
    return jsonify({
        "filters": filters,
        "preferences": prefs
    })

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
            return jsonify({"status": "error", "message": "Failed to save preferences"}), 500

# ==================== Stream Proxying (Future) ====================
@app.route("/lanes/<int:lane_id>/stream.m3u8")
def lane_stream(lane_id):
    """
    Stream endpoint for a lane
    TODO: Implement actual stream proxying based on current schedule
    """
    # For now, return a placeholder
    # In the future, this will:
    # 1. Check what event is currently scheduled on this lane
    # 2. Proxy to the actual provider's stream
    # 3. Handle authentication/deeplinks
    
    return jsonify({
        "error": "Stream proxying not yet implemented",
        "lane_id": lane_id,
        "message": "Use direct deeplinks for now"
    }), 501

@app.route("/api/lanes/<int:lane_id>/schedule")
def lane_schedule(lane_id):
    """Get current and upcoming schedule for a lane"""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        now = datetime.now().isoformat()
        
        cur.execute("""
            SELECT le.*, e.title, e.channel_name, e.synopsis
            FROM lane_events le
            LEFT JOIN events e ON le.event_id = e.id
            WHERE le.lane_id = ?
              AND le.end_utc >= ?
            ORDER BY le.start_utc
            LIMIT 10
        """, (lane_id, now))
        
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
    return jsonify({
        "status": "healthy" if db_ok else "degraded",
        "database": "ok" if db_ok else "missing",
        "timestamp": datetime.now().isoformat()
    })

# ==================== Admin HTML Template ====================
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
                    dbStats.innerHTML = `
                        <div class="stat"><span class="stat-label">Total Events</span><span class="stat-value">${db.total_events || 0}</span></div>
                        <div class="stat"><span class="stat-label">Future Events</span><span class="stat-value">${db.future_events || 0}</span></div>
                        <div class="stat"><span class="stat-label">Lanes</span><span class="stat-value">${db.lane_count || 0}</span></div>
                        <div class="stat"><span class="stat-label">Scheduled</span><span class="stat-value">${db.scheduled_events || 0}</span></div>
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
                    if (refresh.last_run) {
                        const status = refresh.last_status === 'success' 
                            ? '<span class="status-success">✓ Success</span>'
                            : '<span class="status-failed">✗ Failed</span>';
                        refreshStatus.innerHTML = `Last run: ${new Date(refresh.last_run).toLocaleString()} ${status}`;
                    } else {
                        refreshStatus.innerHTML = 'No refresh run yet';
                    }
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

# ==================== Main ====================
if __name__ == "__main__":
    log("FruitDeepLinks server starting...", "INFO")
    
    port = int(os.getenv("PORT", 6655))
    host = os.getenv("HOST", "0.0.0.0")
    
    log(f"Server running on http://{host}:{port}", "INFO")
    log(f"Admin dashboard: http://{host}:{port}/", "INFO")
    
    app.run(host=host, port=port, debug=False, threaded=True)
