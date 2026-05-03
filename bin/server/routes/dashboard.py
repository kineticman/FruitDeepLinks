#!/usr/bin/env python3
"""
routes/dashboard.py - HTML page routes and static file serving

Routes:
  GET /
  GET /api  (API helper page)
  GET /adb
  GET /events
  GET /events/now
  GET /events/<event_id>
  GET /filters
  GET /out/<filename>
  GET /xmltv/lanes
  GET /m3u/lanes
  GET /m3u/adb[?profile=fire|android|apple]
  GET /xmltv/direct
  GET /m3u/direct
  GET /lanes/<lane_id>/stream.m3u8
"""

from flask import Blueprint, jsonify, redirect, request, send_file

from server.config import cfg

try:
    from version_info import PROJECT_URL, get_version
except ImportError:
    def get_version(): return "unknown"
    PROJECT_URL = ""

bp = Blueprint("dashboard", __name__)


@bp.route("/")
def index():
    return _render("admin_dashboard.html",
                   project_version=get_version(), project_url=PROJECT_URL)


@bp.route("/api")
def api_helper():
    return _render("api_helper.html")


@bp.route("/adb")
def adb_config_page():
    return _render("adb_config.html")


@bp.route("/settings")
def settings_page():
    return _render("settings.html")


@bp.route("/filters")
def filters_page():
    return _render("filters.html")


@bp.route("/events")
def events_page():
    return _render("events.html")


@bp.route("/events/now")
def events_now_redirect():
    return redirect("/events?live=1&has_playables=1")


@bp.route("/events/<path:event_id>")
def event_detail_page(event_id):
    return _render("event_detail.html")


# ---- Generated file downloads ----

@bp.route("/out/<filename>")
def serve_file(filename):
    fp = cfg.OUT_DIR / filename
    if not fp.exists():
        return jsonify({"error": "File not found"}), 404
    return send_file(str(fp), as_attachment=False)


@bp.route("/xmltv/lanes")
def serve_lanes_xmltv():
    return send_file(str(cfg.OUT_DIR / "peacock_lanes.xml"))


@bp.route("/m3u/lanes")
def serve_lanes_m3u():
    return send_file(str(cfg.OUT_DIR / "peacock_lanes.m3u"))


@bp.route("/xmltv/direct")
def serve_direct_xmltv():
    return send_file(str(cfg.OUT_DIR / "direct.xml"))


@bp.route("/m3u/direct")
def serve_direct_m3u():
    return send_file(str(cfg.OUT_DIR / "direct.m3u"))


@bp.route("/m3u/adb")
def serve_adb_m3u():
    """ADB lanes M3U — ?profile=fire|android (default) or apple."""
    profile = (request.args.get("profile") or "fire").lower()
    filename = "adb_lanes_apple.m3u" if profile == "apple" else "adb_lanes.m3u"
    fp = cfg.OUT_DIR / filename
    if not fp.exists():
        return jsonify({"error": f"{filename} not found — run export first"}), 404
    return send_file(str(fp), mimetype="audio/x-mpegurl")


@bp.route("/lanes/<int:lane_id>/stream.m3u8")
def lane_stream(lane_id):
    return jsonify({
        "error": "Stream proxying not yet implemented",
        "lane_id": lane_id,
        "message": "Use direct deeplinks for now",
    }), 501


@bp.route("/lane/<int:lane_number>/stream.m3u8", methods=["GET", "HEAD"])
def serve_lane_hls(lane_number):
    """CDVR Detector HLS proxy — requires CDVR_DVR_PATH env var to be set."""
    from server.config import cfg
    if not cfg.DETECTOR_ENABLED:
        return "CDVR Detector not enabled. Set CDVR_DVR_PATH to enable.", 503
    # Full detector logic deferred; the env var gates this feature.
    return "CDVR Detector not yet ported to v2 server.", 503


@bp.route("/lane/<int:lane_number>/segment.ts")
def serve_segment(lane_number):
    """CDVR Detector dummy segment — requires CDVR_DVR_PATH env var to be set."""
    from server.config import cfg
    if not cfg.DETECTOR_ENABLED:
        return "Segment not available", 404
    return "CDVR Detector not yet ported to v2 server.", 503


# ---- Private ----

def _render(template_name: str, **ctx):
    """Render a template from the configured template directory."""
    from flask import current_app, render_template
    return render_template(template_name, **ctx)
