#!/usr/bin/env python3
"""
routes/api/lanes.py - Provider lanes, ADB configuration, and virtual lane endpoints

Routes:
  GET  /api/provider_lanes
  POST /api/provider_lanes
  DELETE /api/provider_lanes/<provider_code>
  GET  /api/lane/<lane_number>/deeplink
  GET  /api/lane/<lane_number>/launch
  GET  /api/adb/lanes/<provider_code>/<lane_number>/deeplink
  GET  /api/lanes
  GET  /api/lanes/<lane_id>/schedule
  GET  /whatson/<lane_id>
  GET  /whatson/all
"""

import sqlite3
from datetime import datetime as _dt

from flask import Blueprint, Response, jsonify, redirect, request

from db.connection import db_exists, get_conn
from server.logging_setup import log
from server.services.lanes import (
    ensure_logo_url_column,
    get_current_events_by_lane,
    get_event_link_columns,
    get_event_link_info,
    get_playable_id_for_event,
    get_provider_lane_stats,
    get_provider_playable_link,
    migrate_aliases,
    normalize_provider_code,
    resolve_whatson,
)

bp = Blueprint("lanes_api", __name__)


# ---- Provider lanes config ----

@bp.route("/api/provider_lanes", methods=["GET", "POST"])
def api_provider_lanes():
    if not db_exists():
        return jsonify({"status": "error", "message": "Database not found"}), 500

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        migrate_aliases(conn)
        ensure_logo_url_column(conn)

        if request.method == "GET":
            try:
                providers = get_provider_lane_stats(conn)
                return jsonify({"status": "success", "providers": providers})
            except Exception as e:
                log(f"Error getting provider lane stats: {e}", "ERROR")
                cur = conn.cursor()
                cur.execute(
                    "SELECT provider_code, adb_enabled, adb_lane_count,"
                    " created_at, updated_at, logo_url FROM provider_lanes ORDER BY provider_code"
                )
                return jsonify({"status": "success", "providers": [dict(r) for r in cur.fetchall()]})

        # POST
        payload = request.get_json(silent=True) or {}
        providers = payload.get("providers")
        if providers is None and isinstance(payload, list):
            providers = payload
        if not isinstance(providers, list):
            return jsonify({"status": "error",
                            "message": 'Expected JSON list or {"providers": [...]} payload'}), 400

        cur = conn.cursor()
        updated = 0
        for item in providers:
            if not isinstance(item, dict):
                continue
            code = normalize_provider_code((item.get("provider_code") or "").strip())
            if not code:
                continue
            enabled_raw = item.get("adb_enabled")
            adb_enabled = 1 if enabled_raw in (1, True, "1", "true", "True") else 0
            try:
                adb_lane_count = int(item.get("adb_lane_count") or 0)
            except (TypeError, ValueError):
                adb_lane_count = 0
            logo_url = (item.get("logo_url") or "").strip() or None

            cur.execute(
                """
                INSERT INTO provider_lanes (provider_code, adb_enabled, adb_lane_count, logo_url, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(provider_code) DO UPDATE SET
                    adb_enabled = excluded.adb_enabled,
                    adb_lane_count = excluded.adb_lane_count,
                    logo_url = excluded.logo_url,
                    updated_at = datetime('now')
                """,
                (code, adb_enabled, adb_lane_count, logo_url),
            )
            updated += 1

        conn.commit()
        log(f"Updated provider_lanes for {updated} provider(s)", "INFO")
        return jsonify({"status": "success", "updated": updated})


@bp.route("/api/provider_lanes/<provider_code>", methods=["DELETE"])
def api_delete_provider_lane(provider_code):
    code = normalize_provider_code(provider_code)
    if not code:
        return jsonify({"status": "error", "error": "Missing provider_code"}), 400
    if not db_exists():
        return jsonify({"status": "error", "error": "Database not found"}), 500

    with get_conn() as conn:
        cur = conn.cursor()
        try:
            if code == "aiv":
                cur.execute(
                    "DELETE FROM adb_lanes WHERE provider_code = ? OR provider_code LIKE 'aiv_%'",
                    (code,),
                )
            else:
                cur.execute("DELETE FROM adb_lanes WHERE provider_code = ?", (code,))
        except Exception:
            pass
        cur.execute("DELETE FROM provider_lanes WHERE provider_code = ?", (code,))
        deleted = cur.rowcount or 0
        conn.commit()

    if deleted == 0:
        return jsonify({"status": "not_found", "provider_code": code}), 404

    log(f"Deleted provider_lanes entry for {code}", "INFO")
    return jsonify({"status": "success", "provider_code": code, "deleted": deleted})


# ---- Virtual lane deeplink / launch ----

@bp.route("/api/lane/<int:lane_number>/deeplink")
def api_lane_deeplink(lane_number):
    fmt = (request.args.get("format") or "text").lower()
    deeplink_format = (request.args.get("deeplink_format") or "scheme").lower()
    at_ts = request.args.get("at") or _dt.utcnow().isoformat(timespec="seconds")

    if not db_exists():
        if fmt == "text":
            return Response("", mimetype="text/plain"), 404
        return jsonify({"deeplink": None, "title": None, "event_id": None,
                        "lane_number": lane_number}), 404

    try:
        with get_conn() as conn:
            data = resolve_whatson(conn, lane_number, at_ts, want_deeplink=True,
                                   deeplink_format=deeplink_format)
    except Exception as e:
        log(f"api_lane_deeplink error: {e}", "ERROR")
        if fmt == "text":
            return Response("", mimetype="text/plain"), 500
        return jsonify({"error": str(e)}), 500

    deeplink = data.get("deeplink_url") or data.get("deeplink_url_full")
    title = data.get("title")
    event_uid = data.get("event_uid")

    if fmt == "html":
        if not deeplink:
            return "<html><body>No event currently scheduled</body></html>", 404
        return Response(
            f'<html><body><a href="{deeplink}">{deeplink}</a></body></html>',
            mimetype="text/html",
        )
    if fmt == "json":
        return jsonify({"deeplink": deeplink, "title": title,
                        "event_id": event_uid, "lane_number": lane_number})
    # text (default)
    if not deeplink:
        return Response("", mimetype="text/plain"), 404
    return Response(deeplink, mimetype="text/plain")


@bp.route("/api/lane/<int:lane_number>/launch")
def api_lane_launch(lane_number):
    deeplink_format = (request.args.get("deeplink_format") or "http").lower()
    allow_fallback = (request.args.get("allow_fallback") or "").lower() in ("1", "true", "yes", "y")
    at_ts = request.args.get("at") or _dt.utcnow().isoformat(timespec="seconds")

    if not db_exists():
        return Response("", mimetype="text/plain"), 404

    try:
        with get_conn() as conn:
            data = resolve_whatson(conn, lane_number, at_ts, want_deeplink=True,
                                   deeplink_format=deeplink_format)
    except Exception as e:
        log(f"api_lane_launch error: {e}", "ERROR")
        return Response("", mimetype="text/plain"), 500

    if not data.get("ok") or not data.get("event_uid"):
        return Response("", mimetype="text/plain"), 404
    if data.get("is_fallback") and not allow_fallback:
        return Response("", mimetype="text/plain"), 404

    deeplink = (
        data.get("deeplink_url_full") or data.get("deeplink_url") or ""
    ).strip()
    if not deeplink:
        return Response("", mimetype="text/plain"), 404
    if not (deeplink.startswith("http://") or deeplink.startswith("https://")):
        log(f"LANE_LAUNCH lane={lane_number} rejected non-http deeplink", "WARNING")
        return Response("", mimetype="text/plain"), 404

    log(f"LANE_LAUNCH lane={lane_number} -> {deeplink}", "INFO")
    r = redirect(deeplink, code=302)
    r.headers["Cache-Control"] = "no-store"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


# ---- ADB provider lane deeplink ----

@bp.route("/api/adb/lanes/<provider_code>/<int:lane_number>/deeplink")
def api_adb_lane_deeplink(provider_code, lane_number):
    fmt = (request.args.get("format") or "text").lower()
    deeplink_format = (request.args.get("deeplink_format") or "scheme").lower()
    at_ts = request.args.get("at") or _dt.utcnow().isoformat(timespec="seconds")

    if not db_exists():
        if fmt == "text":
            return Response("", mimetype="text/plain")
        return jsonify({"status": "error", "message": "Database not found"}), 404

    def _empty_json(msg):
        return jsonify({"status": "success", "deeplink": None, "title": None,
                        "provider_code": provider_code, "lane_number": lane_number,
                        "message": msg})

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # Check provider filter
            try:
                from filter_integration import load_user_preferences
                prefs = load_user_preferences(conn)
                enabled = prefs.get("enabled_services") or []
                if enabled:
                    try:
                        from adb_provider_mapper import get_logical_services_for_adb_provider
                        mapped = get_logical_services_for_adb_provider(provider_code)
                        if not any(ls in enabled for ls in mapped):
                            if fmt == "text":
                                return Response("", mimetype="text/plain")
                            return _empty_json(f"Provider {provider_code} filtered out")
                    except ImportError:
                        if provider_code not in enabled:
                            if fmt == "text":
                                return Response("", mimetype="text/plain")
                            return _empty_json(f"Provider {provider_code} filtered out")
            except Exception as e:
                log(f"ADB provider filter check failed: {e}", "WARNING")

            # Find current adb_lanes row
            cur.execute(
                """
                SELECT event_id, channel_id, start_utc, stop_utc
                FROM adb_lanes
                WHERE provider_code = ? AND lane_number = ?
                  AND datetime(start_utc) <= datetime(?) AND datetime(stop_utc) > datetime(?)
                ORDER BY start_utc DESC LIMIT 1
                """,
                (provider_code, lane_number, at_ts, at_ts),
            )
            adb_row = cur.fetchone()
            if not adb_row:
                if fmt == "text":
                    return Response("", mimetype="text/plain")
                return _empty_json("No event scheduled at this time")

            event_id_str = adb_row["event_id"]
            channel_id = adb_row["channel_id"]
            start_utc = adb_row["start_utc"]
            stop_utc = adb_row["stop_utc"]

            # Strip source prefix to find event in events table
            event_lookup_id = event_id_str
            for prefix in ("appletv-", "kayo-"):
                if event_lookup_id.startswith(prefix):
                    event_lookup_id = event_lookup_id[len(prefix):]
                    break

            uid_col, _, _ = get_event_link_columns(conn)
            log(
                f"ADB deeplink lookup: provider={provider_code}, lane={lane_number},"
                f" event_id={event_id_str}, lookup={event_lookup_id}",
                "DEBUG",
            )

            cur.execute(
                f"SELECT id, title, channel_name, synopsis, start_utc, end_utc, classification_json"
                f" FROM events WHERE {uid_col} = ? LIMIT 1",
                (event_lookup_id,),
            )
            event_row = cur.fetchone()

            if not event_row:
                if fmt == "text":
                    return Response(event_id_str, mimetype="text/plain")
                return jsonify({
                    "status": "success", "deeplink": event_id_str, "title": None,
                    "provider_code": provider_code, "lane_number": lane_number,
                    "channel_id": channel_id, "start_utc": start_utc, "stop_utc": stop_utc,
                    "message": "Event details not found, returning event_id",
                })

            db_event_id = event_row["id"]
            provider_link = get_provider_playable_link(conn, db_event_id, provider_code)
            deeplink_url = provider_link.get("deeplink")
            espn_graph_id = provider_link.get("espn_graph_id")
            service_name = provider_link.get("service_name")
            channel_name_display = service_name or event_row["channel_name"]

            # ESPN correction
            if espn_graph_id and provider_code.lower() in ("sportscenter", "espn", "espn+"):
                try:
                    from deeplink_converter import generate_espn_scheme_deeplink
                    corrected = generate_espn_scheme_deeplink(espn_graph_id, deeplink_url)
                    if corrected:
                        deeplink_url = corrected
                        parts = espn_graph_id.split(":")
                        if len(parts) >= 2:
                            provider_link["http_deeplink_url"] = (
                                f"https://www.espn.com/watch/player/_/id/{parts[1]}"
                            )
                except ImportError:
                    pass

            # NBA/Gametime: strip query params
            if provider_code.lower() in ("gametime", "nba") and deeplink_url:
                if deeplink_url.startswith("gametime://") and "?" in deeplink_url:
                    deeplink_url = deeplink_url.split("?")[0]

            if not deeplink_url:
                if fmt == "text":
                    return Response("", mimetype="text/plain")
                return jsonify({
                    "status": "success", "channel_id": channel_id,
                    "provider_code": provider_code, "lane_number": lane_number,
                    "title": event_row["title"], "channel_name": channel_name_display,
                    "event_start_utc": start_utc, "event_end_utc": stop_utc,
                    "start_utc": start_utc, "stop_utc": stop_utc,
                    "deeplink": None, "deeplink_format": deeplink_format,
                    "message": f"No playable for provider {provider_code}",
                })

            # HTTP format conversion
            if deeplink_format == "http":
                http_from_db = provider_link.get("http_deeplink_url")
                if http_from_db:
                    deeplink_url = http_from_db
                else:
                    try:
                        from deeplink_converter import generate_http_deeplink
                        playable_uuid = provider_link.get("playable_id") or get_playable_id_for_event(
                            conn, db_event_id, provider_code
                        )
                        league_hint = None
                        if event_row["classification_json"]:
                            try:
                                import json as _json
                                for item in _json.loads(event_row["classification_json"]):
                                    if isinstance(item, dict) and item.get("type") == "league":
                                        league_hint = item.get("value")
                                        break
                            except Exception:
                                pass
                        try:
                            http_ver = generate_http_deeplink(
                                deeplink_url, provider=provider_code,
                                playable_id=playable_uuid, espn_graph_id=espn_graph_id,
                                league=league_hint,
                            )
                        except TypeError:
                            http_ver = generate_http_deeplink(deeplink_url, provider_code)
                        if http_ver:
                            deeplink_url = http_ver
                    except ImportError:
                        pass

            if fmt == "text":
                return Response(deeplink_url or "", mimetype="text/plain")
            return jsonify({
                "status": "success", "deeplink": deeplink_url,
                "title": event_row["title"], "channel_name": channel_name_display,
                "provider_code": provider_code, "lane_number": lane_number,
                "channel_id": channel_id, "start_utc": start_utc, "stop_utc": stop_utc,
                "event_start_utc": event_row["start_utc"], "event_end_utc": event_row["end_utc"],
                "deeplink_format": deeplink_format,
            })

    except Exception as e:
        log(f"Error in api_adb_lane_deeplink: {e}", "ERROR")
        if fmt == "text":
            return Response("", mimetype="text/plain")
        return jsonify({"status": "error", "message": str(e)}), 500


# ---- Virtual lane listing / schedule ----

@bp.route("/api/lanes")
def api_lanes():
    if not db_exists():
        return jsonify({"error": "Database not found"}), 500

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                """
                SELECT le.lane_id, COUNT(*) AS event_count
                FROM lane_events le
                JOIN events e ON le.event_id = e.id
                WHERE datetime(e.end_utc) >= datetime('now')
                GROUP BY le.lane_id ORDER BY le.lane_id
                """
            )
            lane_rows = cur.fetchall()
            current_by_lane = get_current_events_by_lane(conn)

        lanes = [
            {"lane_id": row["lane_id"], "event_count": row["event_count"],
             "current": current_by_lane.get(row["lane_id"])}
            for row in lane_rows
        ]
        return jsonify(lanes)
    except Exception as e:
        log(f"/api/lanes error: {e}", "ERROR")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/lanes/<int:lane_id>/schedule")
def lane_schedule(lane_id):
    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            now = _dt.utcnow().isoformat()
            cur.execute(
                """
                SELECT le.*, e.title, e.channel_name, e.synopsis
                FROM lane_events le
                LEFT JOIN events e ON le.event_id = e.id
                WHERE le.lane_id = ? AND le.end_utc >= ?
                ORDER BY le.start_utc LIMIT 10
                """,
                (lane_id, now),
            )
            schedule = [dict(row) for row in cur.fetchall()]
        return jsonify({"lane_id": lane_id, "schedule": schedule})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---- Whatson endpoints ----

@bp.route("/whatson/<int:lane_id>")
def whatson_lane(lane_id):
    if not db_exists():
        if (request.args.get("format") or "").lower() == "txt":
            return Response("", mimetype="text/plain")
        return jsonify({"ok": False, "error": "Database not found"}), 500

    at_ts = request.args.get("at") or _dt.utcnow().isoformat(timespec="seconds")
    want_deeplink = (
        request.args.get("include") == "deeplink"
        or request.args.get("deeplink") in ("1", "true", "yes")
        or request.args.get("dynamic") in ("1", "true", "yes")
    )
    deeplink_format = (request.args.get("deeplink_format") or "scheme").lower()
    fmt = (request.args.get("format") or "json").lower()
    param = request.args.get("param") or "event_uid"

    try:
        with get_conn() as conn:
            data = resolve_whatson(conn, lane_id, at_ts,
                                   want_deeplink=want_deeplink or fmt == "txt",
                                   deeplink_format=deeplink_format)
    except Exception as e:
        log(f"/whatson/{lane_id} error: {e}", "ERROR")
        if fmt == "txt":
            return Response("", mimetype="text/plain")
        return jsonify({"ok": False, "error": str(e)}), 500

    if fmt == "txt":
        if param == "event_uid":
            value = data.get("event_uid") or ""
        elif param == "deeplink_url_full":
            value = data.get("deeplink_url_full") or data.get("deeplink_url") or ""
        else:
            value = data.get("deeplink_url") or data.get("deeplink_url_full") or ""
        return Response(value, mimetype="text/plain")

    payload = {
        "ok": data["ok"],
        "lane": lane_id,
        "event_uid": data.get("event_uid"),
        "at": at_ts,
    }
    if data.get("title"):
        payload["title"] = data["title"]
    if want_deeplink:
        payload["deeplink_url"] = data.get("deeplink_url")
        payload["deeplink_url_full"] = data.get("deeplink_url_full")
    if data.get("is_fallback"):
        payload["is_fallback"] = True
    return jsonify(payload)


@bp.route("/whatson/all")
def whatson_all():
    if not db_exists():
        return jsonify({"ok": False, "error": "Database not found"}), 500

    at_ts = request.args.get("at") or _dt.utcnow().isoformat(timespec="seconds")
    want_deeplink = (
        request.args.get("include") == "deeplink"
        or request.args.get("deeplink") in ("1", "true", "yes")
        or request.args.get("dynamic") in ("1", "true", "yes")
    )

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            current_by_lane = get_current_events_by_lane(conn, at_ts=at_ts)
            uid_col, primary_col, full_col = get_event_link_columns(conn)

            items = []
            for lane_id, row in sorted(current_by_lane.items()):
                event_id = row.get("event_id")
                event_uid = deeplink_url = deeplink_url_full = None
                if event_id is not None:
                    link = get_event_link_info(conn, event_id, uid_col, primary_col, full_col)
                    event_uid = link["event_uid"]
                    deeplink_url = link["deeplink_url"]
                    deeplink_url_full = link["deeplink_url_full"]
                item = {"lane": lane_id, "event_uid": event_uid}
                if want_deeplink:
                    item["deeplink_url"] = deeplink_url
                    item["deeplink_url_full"] = deeplink_url_full
                items.append(item)

        return jsonify({"ok": True, "at": at_ts, "items": items})
    except Exception as e:
        log(f"/whatson/all error: {e}", "ERROR")
        return jsonify({"ok": False, "error": str(e)}), 500
