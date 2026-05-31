#!/usr/bin/env python3
"""
routes/api/filters.py - Filter and preference endpoints

Routes:
  GET  /api/filters
  GET  /api/filters/priorities
  POST /api/filters/priorities
  GET  /api/filters/selection-examples
  GET  /api/filters/preferences
  POST /api/filters/preferences
  POST /api/filters/clear-stale
  POST /api/filters/reset
"""

from flask import Blueprint, jsonify, request

from server.logging_setup import log
from server.services.filters import (
    clear_stale_services,
    expand_amazon,
    get_available_filters,
    get_preferences,
    save_preferences,
)

bp = Blueprint("filters_api", __name__)


@bp.route("/api/filters")
def api_filters():
    """
    Returns { filters: {...}, preferences: {...} } plus top-level filter keys
    for backward compatibility with ADBTuner and other clients.
    """
    filters = get_available_filters()
    prefs = get_preferences()
    payload = {"filters": filters, "preferences": prefs}
    payload.update(filters)   # back-compat: top-level providers/sports/leagues keys
    return jsonify(payload)


@bp.route("/api/filters/priorities", methods=["GET", "POST"])
def api_filter_priorities():
    if request.method == "GET":
        prefs = get_preferences()
        return jsonify({
            "service_priorities": prefs.get("service_priorities", {}),
            "amazon_penalty": prefs.get("amazon_penalty", True),
        })

    data = request.json or {}
    prefs = get_preferences()
    if "service_priorities" in data:
        prefs["service_priorities"] = data["service_priorities"]
    if "amazon_penalty" in data:
        prefs["amazon_penalty"] = bool(data["amazon_penalty"])
    if save_preferences(prefs):
        log("Service priorities updated", "INFO")
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Failed to save priorities"}), 500


@bp.route("/api/filters/selection-examples")
def api_selection_examples():
    """Show which service would win for multi-provider events under current prefs."""
    try:
        from db.connection import get_conn, db_exists
        if not db_exists():
            return jsonify({"examples": []})

        prefs = get_preferences()
        enabled = prefs.get("enabled_services", [])
        amazon_master = prefs.get("amazon_master_enabled", True)
        priority_map = prefs.get("service_priorities", {})
        amazon_penalty = prefs.get("amazon_penalty", True)

        enabled_expanded = expand_amazon(enabled)
        if not amazon_master:
            enabled_expanded = [
                s for s in enabled_expanded
                if s != "aiv" and not s.startswith("aiv_")
            ]

        examples = []
        with get_conn() as conn:
            import sqlite3 as _sq
            conn.row_factory = _sq.Row
            cur = conn.cursor()

            cur.execute("""
                SELECT e.id, e.title, e.channel_name, e.start_utc,
                       COUNT(DISTINCT p.logical_service) AS service_count
                FROM events e
                JOIN playables p ON e.id = p.event_id
                WHERE datetime(e.end_utc) > datetime('now')
                  AND p.logical_service IS NOT NULL
                GROUP BY e.id
                HAVING service_count > 1
                ORDER BY service_count DESC, e.start_utc ASC
                LIMIT 10
            """)
            multi_rows = cur.fetchall()

            for row in multi_rows:
                event_id = row["id"]

                # Get all playables for this event
                cur.execute("""
                    SELECT DISTINCT logical_service, provider, deeplink_play, http_deeplink_url
                    FROM playables
                    WHERE event_id = ?
                      AND logical_service IS NOT NULL
                    ORDER BY priority ASC
                """, (event_id,))
                playable_rows = cur.fetchall()

                available_services = [r["logical_service"] for r in playable_rows]

                # Determine winner under current prefs
                winner = None
                if enabled_expanded:
                    candidates = [s for s in available_services if s in enabled_expanded]
                else:
                    candidates = available_services

                if not amazon_master:
                    candidates = [s for s in candidates if s != "aiv" and not s.startswith("aiv_")]

                if candidates:
                    from core.service_catalog import get_internal_priority, get_display_name
                    def _score(svc):
                        user_p = priority_map.get(svc)
                        if user_p is not None:
                            return -int(user_p)   # higher user priority = lower score = wins
                        base = get_internal_priority(svc)
                        if amazon_penalty and (svc == "aiv" or svc.startswith("aiv_")):
                            base += 10
                        return base
                    winner = min(candidates, key=_score)

                from core.service_catalog import get_display_name
                examples.append({
                    "event_id": event_id,
                    "title": row["title"],
                    "channel": row["channel_name"],
                    "start": row["start_utc"],
                    "available_services": [
                        {"code": s, "name": get_display_name(s)} for s in available_services
                    ],
                    "selected_service": winner,
                    "selected_name": get_display_name(winner) if winner else None,
                })

        return jsonify({"examples": examples})

    except Exception as e:
        log(f"Error in selection examples: {e}", "ERROR")
        return jsonify({"examples": [], "error": str(e)})


@bp.route("/api/filters/preferences", methods=["GET", "POST"])
def api_filters_preferences():
    if request.method == "GET":
        return jsonify(get_preferences())

    prefs = request.json
    if save_preferences(prefs):
        log("Filter preferences updated", "INFO")
        return jsonify({"status": "success"})
    return jsonify({"status": "error", "message": "Failed to save preferences"}), 500


@bp.route("/api/filters/clear-stale", methods=["POST"])
def api_filters_clear_stale():
    result = clear_stale_services()
    if result.get("status") == "error":
        return jsonify(result), 500
    return jsonify(result)


@bp.route("/api/filters/reset", methods=["POST"])
def api_filters_reset():
    try:
        prefs = get_preferences()
        prefs["enabled_services"] = []
        prefs["disabled_sports"] = []
        prefs["disabled_leagues"] = []
        prefs["amazon_master_enabled"] = True
        if save_preferences(prefs):
            log("Filter preferences reset to defaults", "INFO")
            return jsonify({"status": "ok"})
        return jsonify({"status": "error", "message": "Failed to save preferences"}), 500
    except Exception as e:
        log(f"Error resetting preferences: {e}", "ERROR")
        return jsonify({"status": "error", "message": str(e)}), 500
