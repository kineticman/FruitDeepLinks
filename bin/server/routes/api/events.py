#!/usr/bin/env python3
"""
routes/api/events.py - Event search and detail endpoints

Routes:
  GET /api/events
  GET /api/events/stats
  GET /api/events/<event_id>
"""

import sqlite3

from flask import Blueprint, jsonify

from db.connection import db_exists, get_conn
from server.logging_setup import log
from server.utils import db_has_column, parse_int_arg, pretty_json, row_to_dict

bp = Blueprint("events_api", __name__)

try:
    from filter_integration import (
        get_best_deeplink_for_event,
        get_filtered_playables,
        load_user_preferences,
    )
    from server.services.filters import expand_amazon
    _FILTER_AVAILABLE = True
except ImportError:
    _FILTER_AVAILABLE = False


@bp.route("/api/events")
def api_events():
    if not db_exists():
        return jsonify({"ok": False, "error": "Database not found"}), 404

    q = _str_arg("q")
    provider = _str_arg("provider")
    sort = _str_arg("sort") or "start_desc"
    page = parse_int_arg("page", 1, 1, 99999)
    page_size = parse_int_arg("page_size", 50, 1, 500)
    days_back = parse_int_arg("days_back", 2, 0, 90)
    days_forward = parse_int_arg("days_forward", 7, 0, 90)

    live = parse_int_arg("live", 0, 0, 1)
    has_playables = parse_int_arg("has_playables", 0, 0, 1)
    multi = parse_int_arg("multi", 0, 0, 1)
    missing_http = parse_int_arg("missing_http", 0, 0, 1)
    premium = parse_int_arg("premium", 0, 0, 1)
    free = parse_int_arg("free", 0, 0, 1)

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            has_logical = db_has_column(conn, "playables", "logical_service")
            svc = "COALESCE(p.logical_service, p.provider)" if has_logical else "p.provider"

            where, params = _build_where(
                q, provider, svc, days_back, days_forward,
                live, has_playables, multi, missing_http, premium, free,
                conn
            )
            where_sql = ("WHERE " + " AND ".join(where)) if where else ""
            order_sql = _build_order(sort)

            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM events e {where_sql}", params)
            total = int(cur.fetchone()[0] or 0)

            offset = (page - 1) * page_size
            cur.execute(
                f"""
                SELECT e.id, e.title, e.start_utc, e.end_utc, e.channel_name,
                       e.last_seen_utc,
                       COALESCE(e.is_free, 0) AS is_free,
                       COALESCE(e.is_premium, 0) AS is_premium,
                       (SELECT COUNT(*) FROM playables p WHERE p.event_id = e.id) AS playables_count,
                       (SELECT GROUP_CONCAT(DISTINCT {svc}) FROM playables p WHERE p.event_id = e.id) AS providers_csv,
                       CASE WHEN datetime(e.start_utc) <= datetime('now')
                                 AND datetime(e.end_utc) > datetime('now') THEN 1 ELSE 0 END AS is_live_now
                FROM events e
                {where_sql}
                {order_sql}
                LIMIT ? OFFSET ?
                """,
                params + [page_size, offset],
            )
            items = []
            for row in cur.fetchall():
                d = row_to_dict(row)
                csv_ = d.pop("providers_csv", "") or ""
                d["providers"] = [p for p in csv_.split(",") if p]
                items.append(d)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True, "page": page, "page_size": page_size, "total": total, "items": items})


@bp.route("/api/events/stats")
def api_events_stats():
    if not db_exists():
        return jsonify({"ok": False, "error": "Database not found"}), 404

    days_back = parse_int_arg("days_back", 2, 0, 90)
    days_forward = parse_int_arg("days_forward", 7, 0, 90)

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            has_logical = db_has_column(conn, "playables", "logical_service")
            svc = "COALESCE(p.logical_service, p.provider)" if has_logical else "p.provider"
            ww = "WHERE datetime(e.end_utc) >= datetime('now', ?) AND datetime(e.start_utc) <= datetime('now', ?)"
            wp = [f"-{days_back} days", f"+{days_forward} days"]

            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM events e {ww}", wp)
            window_total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""SELECT COUNT(*) FROM events e {ww}
                AND datetime(e.start_utc) <= datetime('now')
                AND datetime(e.end_utc) > datetime('now')
                AND (SELECT COUNT(*) FROM playables p WHERE p.event_id = e.id) > 0""",
                wp,
            )
            live_now = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""SELECT COUNT(*) FROM events e {ww}
                AND (SELECT COUNT(DISTINCT {svc}) FROM playables p WHERE p.event_id = e.id) >= 2""",
                wp,
            )
            multi_service = int(cur.fetchone()[0] or 0)

            missing_http = 0
            if db_has_column(conn, "playables", "http_deeplink_url"):
                cur.execute(
                    f"""SELECT COUNT(*) FROM events e {ww}
                    AND EXISTS (SELECT 1 FROM playables p WHERE p.event_id = e.id
                               AND (p.http_deeplink_url IS NULL OR p.http_deeplink_url = '')
                               AND (p.deeplink_play IS NOT NULL OR p.deeplink_open IS NOT NULL))""",
                    wp,
                )
                missing_http = int(cur.fetchone()[0] or 0)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "window_total": window_total,
        "live_now": live_now,
        "multi_service": multi_service,
        "missing_http": missing_http,
    })


@bp.route("/api/events/<path:event_id>")
def api_event_detail(event_id):
    if not db_exists():
        return jsonify({"ok": False, "error": "Database not found"}), 404

    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            cur.execute("SELECT * FROM events WHERE id = ?", (event_id,))
            event_row = cur.fetchone()
            if not event_row:
                return jsonify({"ok": False, "error": "Event not found"}), 404
            event = row_to_dict(event_row)

            # Playables ordered by priority
            cur.execute("PRAGMA table_info(playables)")
            playable_cols = [r[1] for r in cur.fetchall()]
            order_bits = []
            if "priority" in playable_cols:
                order_bits.append("COALESCE(priority, -999999) DESC")
            if "created_utc" in playable_cols:
                order_bits.append("COALESCE(datetime(created_utc), datetime('now')) DESC")
            if not order_bits:
                order_bits.append("rowid DESC")

            cur.execute(
                f"SELECT * FROM playables WHERE event_id = ? ORDER BY {', '.join(order_bits)}",
                (event_id,),
            )
            playables = [row_to_dict(r) for r in cur.fetchall()]

            # Providers list
            if playables and "logical_service" in playables[0]:
                providers = sorted({
                    p.get("logical_service") or p.get("provider") or ""
                    for p in playables
                    if p.get("logical_service") or p.get("provider")
                })
            else:
                providers = sorted({p.get("provider") or "" for p in playables if p.get("provider")})

            # Is live now?
            is_live_now = False
            try:
                cur.execute(
                    "SELECT CASE WHEN datetime(?) <= datetime('now') AND datetime(?) > datetime('now') THEN 1 ELSE 0 END",
                    (event.get("start_utc"), event.get("end_utc")),
                )
                is_live_now = bool(cur.fetchone()[0])
            except Exception:
                pass

            # Pretty-print JSON fields
            json_keys = ["classification_json", "genres_json", "content_segments_json", "raw_attributes_json"]
            pretty_json_fields = [
                {"key": k, "value": pretty_json(event.get(k))}
                for k in json_keys if k in event
            ]

            # Best deeplink under current prefs
            best = _compute_best(conn, event_id, playables)

    except Exception as e:
        log(f"Error in event detail {event_id}: {e}", "ERROR")
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({
        "ok": True,
        "event": event,
        "playables": playables,
        "providers": providers,
        "is_live_now": is_live_now,
        "pretty_json_fields": pretty_json_fields,
        "best": best,
    })


# ---- Private helpers ----

def _str_arg(name: str) -> str:
    from flask import request
    return (request.args.get(name) or "").strip()


def _build_where(q, provider, svc, days_back, days_forward,
                 live, has_playables, multi, missing_http, premium, free, conn):
    where, params = [], []
    where.append("datetime(e.end_utc) >= datetime('now', ?)")
    params.append(f"-{days_back} days")
    where.append("datetime(e.start_utc) <= datetime('now', ?)")
    params.append(f"+{days_forward} days")

    if q:
        like = f"%{q}%"
        where.append("(" + " OR ".join(
            ["e.title LIKE ?", "e.id LIKE ?", "e.pvid LIKE ?",
             "e.slug LIKE ?", "e.synopsis LIKE ?", "e.synopsis_brief LIKE ?"]
        ) + ")")
        params.extend([like] * 6)

    if provider:
        where.append(f"EXISTS (SELECT 1 FROM playables p WHERE p.event_id = e.id AND {svc} = ?)")
        params.append(provider)

    if has_playables:
        where.append("(SELECT COUNT(*) FROM playables p WHERE p.event_id = e.id) > 0")

    if multi:
        where.append(f"(SELECT COUNT(DISTINCT {svc}) FROM playables p WHERE p.event_id = e.id) >= 2")

    if missing_http:
        if db_has_column(conn, "playables", "http_deeplink_url"):
            where.append(
                "EXISTS (SELECT 1 FROM playables p WHERE p.event_id = e.id "
                "AND (p.http_deeplink_url IS NULL OR p.http_deeplink_url = '') "
                "AND (p.deeplink_play IS NOT NULL OR p.deeplink_open IS NOT NULL))"
            )
        else:
            where.append("0")

    if live:
        where.append("datetime(e.start_utc) <= datetime('now') AND datetime(e.end_utc) > datetime('now')")

    if premium:
        where.append("COALESCE(e.is_premium, 0) = 1")
    if free:
        where.append("COALESCE(e.is_free, 0) = 1")

    return where, params


def _build_order(sort: str) -> str:
    return {
        "start_asc": "ORDER BY datetime(e.start_utc) ASC",
        "seen_desc": "ORDER BY COALESCE(datetime(e.last_seen_utc), datetime(e.created_utc)) DESC",
        "playables_desc": "ORDER BY playables_count DESC, datetime(e.start_utc) DESC",
    }.get(sort, "ORDER BY datetime(e.start_utc) DESC")


def _compute_best(conn, event_id, playables) -> dict | None:
    if not _FILTER_AVAILABLE:
        return None
    try:
        prefs = load_user_preferences(conn)
        enabled = expand_amazon(prefs.get("enabled_services", []))
        amazon_master = prefs.get("amazon_master_enabled", True)

        deeplink = None
        try:
            deeplink = get_best_deeplink_for_event(conn, event_id, enabled)
        except Exception:
            pass

        top = None
        try:
            filtered = get_filtered_playables(conn, event_id, enabled, amazon_master_enabled=amazon_master)
            if filtered:
                top = filtered[0]
        except Exception:
            pass

        if top:
            actual = deeplink or top.get("deeplink_play") or top.get("deeplink_open")
            src = "ESPN Watch Graph" if (deeplink and top.get("espn_graph_id")) else "Apple TV"
            return {
                "provider": top.get("provider"),
                "logical_service": top.get("logical_service"),
                "deeplink": actual,
                "http_deeplink_url": top.get("http_deeplink_url"),
                "espn_graph_id": top.get("espn_graph_id"),
                "deeplink_source": src,
                "reason": "Top of filtered playables order",
            }
        if deeplink:
            match = next(
                (p for p in playables
                 if deeplink and (p.get("deeplink_play") == deeplink or p.get("deeplink_open") == deeplink)),
                None,
            )
            return {
                "provider": match.get("provider") if match else None,
                "logical_service": match.get("logical_service") if match else None,
                "deeplink": deeplink,
                "http_deeplink_url": match.get("http_deeplink_url") if match else None,
                "espn_graph_id": match.get("espn_graph_id") if match else None,
                "deeplink_source": "get_best_deeplink_for_event()",
                "reason": "get_best_deeplink_for_event()",
            }
    except Exception:
        pass
    return None
