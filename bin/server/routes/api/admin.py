#!/usr/bin/env python3
"""
routes/api/admin.py - System management endpoints

Routes:
  GET  /health
  GET  /api/status
  GET  /api/logs
  GET  /api/logs/stream   (SSE)
  POST /api/refresh
  GET  /api/auto-refresh
  POST /api/auto-refresh
  POST /api/apply-filters
  POST /api/wipe-event-data
  GET  /api/settings
  POST /api/settings
"""

import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, Response, jsonify, request, stream_with_context

from db.connection import db_exists, get_conn, resolve_db_path
from db.preferences import get_settings_schema, load_all_settings, save_settings
from db.stats import get_db_stats
from server import scheduler as sched
from server.config import cfg
from server.logging_setup import get_recent_logs, log
from server.refresh import refresh_status, start_apply_filters_thread, start_refresh_thread
from server.services.filters import get_auto_refresh, save_auto_refresh

try:
    from version_info import PROJECT_URL, get_version
except ImportError:
    def get_version(): return "unknown"
    PROJECT_URL = ""

bp = Blueprint("admin", __name__)


@bp.route("/health")
def health():
    ok = db_exists()
    return jsonify({
        "status": "healthy" if ok else "degraded",
        "database": "ok" if ok else "missing",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/api/status")
def api_status():
    stats = get_db_stats()

    # Output files
    files = {}
    out_dir = cfg.OUT_DIR
    if out_dir.exists():
        for fp in list(out_dir.glob("*.xml")) + list(out_dir.glob("*.m3u")):
            if fp.exists():
                s = fp.stat()
                files[fp.name] = {
                    "size": s.st_size,
                    "modified": datetime.fromtimestamp(s.st_mtime).isoformat(),
                }

    auto_settings = get_auto_refresh()
    next_run = sched.next_run_iso()

    # Relevant env vars for the dashboard display
    _env_keys = [
        "SERVER_URL", "FRUIT_HOST_PORT", "CHANNELS_DVR_IP", "CHANNELS_SOURCE_NAME",
        "CDVR_DVR_PATH", "CDVR_SERVER_PORT", "CDVR_API_PORT", "TZ",
        "FRUIT_LANES", "FRUIT_LANE_START_CH", "AUTO_REFRESH_ENABLED", "AUTO_REFRESH_TIME",
        "HEADLESS", "LOG_LEVEL",
        "KAYO_ENABLED", "FANATIZ_ENABLED", "BEIN_ENABLED", "NESN_ENABLED",
        "VICTORY_ENABLED", "GOTHAM_ENABLED", "ESPN_ENABLED",
        "KAYO_DAYS", "NESN_DAYS", "ESPN_DAYS", "GOTHAM_DAYS", "GOTHAM_ZONE",
        "APPLE_AUTH_BOOTSTRAP", "DB_MAINTENANCE",
    ]
    env_vars = {k: os.getenv(k, "") for k in _env_keys if os.getenv(k)}

    return jsonify({
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
        "project_version": get_version(),
        "project_url": PROJECT_URL,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/api/logs")
def api_logs():
    count = min(request.args.get("count", 5000, type=int), 10000)
    lines = get_recent_logs(count=count)
    return jsonify({"logs": [l for _, l in lines], "count": len(lines)})


@bp.route("/api/logs/stream")
def api_logs_stream():
    def generate():
        try:
            tail = int(request.args.get("tail", "0") or "0")
            since = int(request.args.get("since", "0") or "0")
        except Exception:
            tail = since = 0

        last_seq = since
        heartbeat_ts = time.time()
        yield ": connected\n\n"

        if tail > 0:
            for seq, line in get_recent_logs(count=tail):
                yield f"data: {json.dumps({'seq': seq, 'log': line}, ensure_ascii=False)}\n\n"
                last_seq = max(last_seq, seq)

        while True:
            for seq, line in get_recent_logs(count=5000, after_seq=last_seq):
                yield f"data: {json.dumps({'seq': seq, 'log': line}, ensure_ascii=False)}\n\n"
                last_seq = max(last_seq, seq)

            if (time.time() - heartbeat_ts) >= 15:
                yield "event: ping\ndata: {}\n\n"
                heartbeat_ts = time.time()

            time.sleep(0.5)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@bp.route("/api/refresh", methods=["POST"])
def api_refresh():
    if refresh_status["running"]:
        return jsonify({"error": "Refresh already running"}), 409
    skip = bool((request.json or {}).get("skip_scrape", False))
    start_refresh_thread(skip_scrape=skip, source="manual")
    return jsonify({"status": "started"})


@bp.route("/api/auto-refresh", methods=["GET", "POST"])
def api_auto_refresh():
    if request.method == "GET":
        settings = get_auto_refresh()
        return jsonify({
            "enabled": settings.get("enabled", False),
            "time": settings.get("time", "02:30"),
            "next_run": sched.next_run_iso(),
        })

    data = request.json or {}
    enabled = bool(data.get("enabled", False))
    time_str = data.get("time", "02:30")
    try:
        hour, minute = [int(x) for x in time_str.split(":", 1)]
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        time_str = f"{hour:02d}:{minute:02d}"
    except Exception:
        return jsonify({"error": "Invalid time format, use HH:MM (24h)"}), 400

    settings = {"enabled": enabled, "time": time_str}
    if not save_auto_refresh(settings):
        return jsonify({"error": "Failed to save settings"}), 500
    sched.schedule(settings)
    return jsonify({"status": "ok", "next_run": sched.next_run_iso()})


@bp.route("/api/apply-filters", methods=["POST"])
def api_apply_filters():
    if refresh_status["running"]:
        return jsonify({"error": "Refresh already running"}), 409
    start_apply_filters_thread()
    return jsonify({"status": "started"})


@bp.route("/api/wipe-event-data", methods=["POST"])
def api_wipe_event_data():
    if refresh_status["running"]:
        return jsonify({"error": "Cannot wipe while refresh is running"}), 409

    result = {"events_deleted": 0, "playables_deleted": 0, "lanes_deleted": 0, "errors": []}
    db_path = resolve_db_path()
    refresh_status["running"] = True
    refresh_status["current_step"] = "Wiping event data..."
    log("Starting database wipe (preserving settings)", "INFO")

    try:
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            try:
                cur = conn.cursor()
                cur.execute("PRAGMA foreign_keys = ON")

                def _table_exists(t):
                    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,))
                    return cur.fetchone() is not None

                def _count_and_delete(t):
                    if not _table_exists(t):
                        return 0
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    n = cur.fetchone()[0]
                    cur.execute(f"DELETE FROM {t}")
                    return n

                result["events_deleted"] = _count_and_delete("events")
                result["playables_deleted"] = _count_and_delete("playables")
                _count_and_delete("event_images")

                for t in ("lanes", "lane_events", "adb_lanes"):
                    result["lanes_deleted"] += _count_and_delete(t)

                for t in ("amazon_channels", "amazon_channel_history"):
                    _count_and_delete(t)

                conn.commit()
                log(
                    f"Wipe complete: {result['events_deleted']} events, "
                    f"{result['playables_deleted']} playables, "
                    f"{result['lanes_deleted']} lane rows",
                    "INFO",
                )
            finally:
                conn.close()

            # VACUUM in background so it doesn't stall the HTTP response
            def _vacuum():
                try:
                    c = sqlite3.connect(str(db_path))
                    c.execute("VACUUM")
                    c.close()
                except Exception:
                    pass
            threading.Thread(target=_vacuum, daemon=True).start()

        # Remove cache files
        for cache in [
            db_path.parent / "amazon_gti_cache.pkl",
            Path("/app/data/apple_events.db"),
            Path("/app/data/espn_graph.db"),
        ]:
            if cache.exists():
                cache.unlink()
                log(f"Deleted {cache}", "INFO")

    except Exception as e:
        log(f"Wipe error: {e}", "ERROR")
        result["errors"].append(str(e))
    finally:
        refresh_status["running"] = False
        refresh_status["current_step"] = None

    return jsonify(result)


@bp.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    if not db_exists():
        return jsonify({"status": "error", "message": "Database not found"}), 500

    with get_conn() as conn:
        if request.method == "GET":
            current = load_all_settings(conn)
            schema = get_settings_schema()
            return jsonify({"status": "success", "settings": current, "schema": schema})

        updates = request.json or {}
        if not isinstance(updates, dict):
            return jsonify({"status": "error", "message": "Expected JSON object"}), 400

        if save_settings(conn, updates):
            log(f"Settings updated: {list(updates.keys())}", "INFO")
            return jsonify({"status": "success", "updated": list(updates.keys())})
        return jsonify({"status": "error", "message": "Failed to save settings"}), 500
