#!/usr/bin/env python3
"""
server/services/lanes.py - Provider lane and ADB configuration business logic

Extracted from fruitdeeplinks_server.py to eliminate inline route handler code.
"""

import json
import os
import sqlite3
import urllib.parse
from datetime import datetime as _dt, timedelta, timezone
from typing import Optional

from server.logging_setup import log

try:
    from core.service_catalog import get_display_name
    _CATALOG = True
except ImportError:
    _CATALOG = False

try:
    from adb_provider_mapper import get_adb_provider_code
    _ADB_MAPPER = True
except ImportError:
    _ADB_MAPPER = False

# Legacy provider_code aliases: old code -> canonical code
PROVIDER_LANE_ALIASES = {
    "kayo": "kayo_web",
}


def normalize_provider_code(code: str) -> str:
    """Return canonical provider_lanes code.

    - Applies explicit aliases (e.g., 'kayo' -> 'kayo_web')
    - Collapses Amazon sub-services (aiv_*) to 'aiv'
    """
    code = (code or "").strip()
    if not code:
        return ""
    code = PROVIDER_LANE_ALIASES.get(code, code)
    if code.startswith("aiv_"):
        return "aiv"
    return code


def ensure_logo_url_column(conn: sqlite3.Connection) -> None:
    """Add logo_url column to provider_lanes if not present (idempotent)."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(provider_lanes)")
    cols = [r[1] for r in cur.fetchall()]
    if "logo_url" not in cols:
        cur.execute("ALTER TABLE provider_lanes ADD COLUMN logo_url TEXT")
        conn.commit()


def migrate_aliases(conn: sqlite3.Connection) -> None:
    """Move legacy provider_lanes rows to canonical codes (idempotent)."""
    try:
        cur = conn.cursor()
        for legacy, canonical in PROVIDER_LANE_ALIASES.items():
            if legacy == canonical:
                continue
            cur.execute(
                "SELECT provider_code, adb_enabled, adb_lane_count, created_at "
                "FROM provider_lanes WHERE provider_code = ?",
                (legacy,),
            )
            legacy_row = cur.fetchone()
            if not legacy_row:
                continue
            cur.execute("SELECT 1 FROM provider_lanes WHERE provider_code = ? LIMIT 1", (canonical,))
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO provider_lanes (provider_code, adb_enabled, adb_lane_count, "
                    "created_at, updated_at) VALUES (?, ?, ?, COALESCE(?, datetime('now')), datetime('now'))",
                    (canonical, legacy_row[1], legacy_row[2], legacy_row[3]),
                )
            try:
                cur.execute("UPDATE adb_lanes SET provider_code = ? WHERE provider_code = ?", (canonical, legacy))
            except Exception:
                pass
            cur.execute("DELETE FROM provider_lanes WHERE provider_code = ?", (legacy,))

        # Collapse aiv_* sub-services into 'aiv'
        try:
            cur.execute(
                "SELECT provider_code, adb_enabled, adb_lane_count "
                "FROM provider_lanes WHERE provider_code LIKE 'aiv_%' AND provider_code != 'aiv'"
            )
            legacy_rows = cur.fetchall()
            if legacy_rows:
                cur.execute("SELECT adb_enabled, adb_lane_count FROM provider_lanes WHERE provider_code='aiv'")
                existing = cur.fetchone()
                merged_enabled = int(existing[0]) if existing else 0
                merged_lanes = int(existing[1]) if existing else 0
                for _, en, lc in legacy_rows:
                    merged_enabled = max(merged_enabled, int(en or 0))
                    merged_lanes = max(merged_lanes, int(lc or 0))
                if existing:
                    cur.execute(
                        "UPDATE provider_lanes SET adb_enabled=?, adb_lane_count=?, "
                        "updated_at=datetime('now') WHERE provider_code='aiv'",
                        (merged_enabled, merged_lanes),
                    )
                else:
                    cur.execute(
                        "INSERT INTO provider_lanes (provider_code, adb_enabled, adb_lane_count, "
                        "created_at, updated_at) VALUES ('aiv', ?, ?, datetime('now'), datetime('now'))",
                        (merged_enabled, merged_lanes),
                    )
                cur.execute("DELETE FROM provider_lanes WHERE provider_code LIKE 'aiv_%' AND provider_code != 'aiv'")
                try:
                    cur.execute(
                        "UPDATE adb_lanes SET provider_code='aiv' "
                        "WHERE provider_code LIKE 'aiv_%' AND provider_code != 'aiv'"
                    )
                except Exception:
                    pass
        except Exception:
            pass

        conn.commit()
    except Exception as e:
        log(f"provider_lanes alias migration skipped: {e}", "WARNING")


def get_provider_lane_stats(conn: sqlite3.Connection) -> list[dict]:
    """
    Return comprehensive per-provider stats for the ADB Config page.
    Merges playable counts from the events DB with provider_lanes configuration.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(playables)")
    cols = [r[1] for r in cur.fetchall()]

    if "logical_service" in cols:
        cur.execute(
            """
            SELECT p.logical_service AS service_code,
                   COUNT(DISTINCT p.event_id) AS event_count,
                   COUNT(*) AS playable_count,
                   COUNT(DISTINCT CASE WHEN datetime(e.end_utc) > datetime('now') THEN p.event_id END) AS future_event_count
            FROM playables p
            LEFT JOIN events e ON p.event_id = e.id
            WHERE p.logical_service IS NOT NULL AND p.logical_service != ''
            GROUP BY p.logical_service
            ORDER BY event_count DESC
            """
        )
    else:
        cur.execute(
            """
            SELECT p.provider AS service_code,
                   COUNT(DISTINCT p.event_id) AS event_count,
                   COUNT(*) AS playable_count,
                   COUNT(DISTINCT CASE WHEN datetime(e.end_utc) > datetime('now') THEN p.event_id END) AS future_event_count
            FROM playables p
            LEFT JOIN events e ON p.event_id = e.id
            WHERE p.provider IS NOT NULL AND p.provider != ''
            GROUP BY p.provider
            ORDER BY event_count DESC
            """
        )

    services: dict[str, dict] = {}
    for row in cur.fetchall():
        code = row[0]
        name = get_display_name(code) if _CATALOG else code.upper()
        services[code] = {
            "provider_code": code,
            "name": name,
            "event_count": row[1],
            "playable_count": row[2],
            "future_event_count": row[3],
            "adb_enabled": 0,
            "adb_lane_count": 0,
            "logo_url": None,
            "created_at": None,
            "updated_at": None,
        }

    # Merge provider_lanes config
    cur.execute(
        "SELECT provider_code, adb_enabled, adb_lane_count, created_at, updated_at, logo_url "
        "FROM provider_lanes"
    )
    for row in cur.fetchall():
        code = row[0]
        if code in services:
            services[code].update({
                "adb_enabled": row[1], "adb_lane_count": row[2],
                "created_at": row[3], "updated_at": row[4], "logo_url": row[5],
            })
        else:
            name = get_display_name(code) if _CATALOG else code.upper()
            services[code] = {
                "provider_code": code, "name": name,
                "event_count": 0, "playable_count": 0, "future_event_count": 0,
                "adb_enabled": row[1], "adb_lane_count": row[2],
                "logo_url": row[5], "created_at": row[3], "updated_at": row[4],
            }

    # Aggregate by ADB provider code (e.g., espn_linear + espn_plus -> sportscenter)
    agg: dict[str, dict] = {}
    if _ADB_MAPPER:
        for code, info in services.items():
            adb_code = "aiv" if (code == "aiv" or code.startswith("aiv_")) else get_adb_provider_code(code)
            if adb_code not in agg:
                agg[adb_code] = {
                    "provider_code": adb_code,
                    "name": get_display_name(adb_code) if _CATALOG else adb_code.upper(),
                    "event_count": 0, "playable_count": 0, "future_event_count": 0,
                    "adb_enabled": 0, "adb_lane_count": 0,
                    "logo_url": None, "created_at": None, "updated_at": None,
                }
                if adb_code == "aiv":
                    agg[adb_code]["name"] = "Amazon (All AIV)"
            agg[adb_code]["event_count"] += info["event_count"]
            agg[adb_code]["playable_count"] += info["playable_count"]
            agg[adb_code]["future_event_count"] += info["future_event_count"]
            if adb_code == code:
                agg[adb_code].update({
                    "adb_enabled": info["adb_enabled"],
                    "adb_lane_count": info["adb_lane_count"],
                    "logo_url": info["logo_url"],
                    "created_at": info["created_at"],
                    "updated_at": info["updated_at"],
                })
        result = agg
    else:
        # Fallback: just collapse Amazon sub-services
        amazon: Optional[dict] = None
        result = {}
        for code, info in services.items():
            if code == "aiv" or code.startswith("aiv_"):
                if amazon is None:
                    amazon = {
                        "provider_code": "aiv", "name": "Amazon (All AIV)",
                        "event_count": 0, "playable_count": 0, "future_event_count": 0,
                        "adb_enabled": 0, "adb_lane_count": 0,
                        "logo_url": None, "created_at": None, "updated_at": None,
                    }
                amazon["event_count"] += info["event_count"]
                amazon["playable_count"] += info["playable_count"]
                amazon["future_event_count"] += info["future_event_count"]
                if code == "aiv":
                    amazon.update({
                        "adb_enabled": info["adb_enabled"],
                        "adb_lane_count": info["adb_lane_count"],
                        "logo_url": info["logo_url"],
                    })
                continue
            result[code] = info
        if amazon:
            result["aiv"] = amazon

    return sorted(result.values(), key=lambda x: (-x["event_count"], x["name"]))


# ---- Lane query helpers (extracted from monolith) ----

def get_event_link_columns(conn: sqlite3.Connection):
    """Inspect events table; return (uid_col, primary_deeplink_col, full_deeplink_col)."""
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(events)")
        rows = cur.fetchall()
    except Exception:
        return "id", None, None

    cols = {row[1] for row in rows}

    uid_col = "id"
    for candidate in ("event_uid", "event_id", "uid", "pvid"):
        if candidate in cols:
            uid_col = candidate
            break

    primary = None
    if "deeplink_url" in cols:
        primary = "deeplink_url"
    elif "deeplink" in cols:
        primary = "deeplink"
    elif "deeplink_url_full" in cols:
        primary = "deeplink_url_full"

    full = "deeplink_url_full" if "deeplink_url_full" in cols else None
    return uid_col, primary, full


def get_provider_playable_link(conn: sqlite3.Connection, event_id: str, provider_code: str) -> dict:
    """Return provider-specific deeplink info for an event from playables.

    Bypasses enabled_services selection — used by provider-based ADB lanes.
    """
    empty = {"deeplink": None, "http_deeplink_url": None, "playable_id": None,
             "espn_graph_id": None, "service_name": None}
    if not event_id or not provider_code:
        return empty

    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(playables)")
        cols = {row[1] for row in cur.fetchall()}

        provider_col = "provider" if "provider" in cols else ("provider_code" if "provider_code" in cols else None)
        event_fk = "event_id" if "event_id" in cols else ("id" if "id" in cols else None)
        if not provider_col or not event_fk:
            return empty

        logical_col = "logical_service" if "logical_service" in cols else None
        playable_id_col = "playable_id" if "playable_id" in cols else None
        http_col = "http_deeplink_url" if "http_deeplink_url" in cols else None
        priority_col = "priority" if "priority" in cols else None
        espn_col = "espn_graph_id" if "espn_graph_id" in cols else None
        svc_name_col = "service_name" if "service_name" in cols else None

        deeplink_cols = [c for c in ["deeplink_play", "deeplink_open", "playable_url"] if c in cols]
        if not deeplink_cols:
            return empty

        select_cols = deeplink_cols[:]
        for extra in (http_col, playable_id_col, espn_col, svc_name_col):
            if extra:
                select_cols.append(extra)

        params = [event_id]
        if logical_col:
            try:
                from adb_provider_mapper import get_logical_services_for_adb_provider
                mapped = get_logical_services_for_adb_provider(provider_code)
                placeholders = ",".join("?" * len(mapped))
                where = f"{event_fk} = ? AND {logical_col} IN ({placeholders})"
                params.extend(mapped)
            except ImportError:
                where = f"{event_fk} = ? AND ({logical_col} = ? OR {provider_col} = ?)"
                params.extend([provider_code, provider_code])
        else:
            where = f"{event_fk} = ? AND {provider_col} = ?"
            params.append(provider_code)

        is_espn = provider_code.lower() in ("sportscenter", "espn", "espn+")
        if is_espn and espn_col:
            order = f"ORDER BY CASE WHEN {espn_col} IS NOT NULL AND {espn_col} != '' THEN 0 ELSE 1 END"
            if svc_name_col:
                order += (
                    f", CASE LOWER({svc_name_col})"
                    " WHEN 'espn' THEN 0 WHEN 'espn deportes' THEN 1"
                    " WHEN 'espn2' THEN 2 WHEN 'espnu' THEN 2"
                    " WHEN 'espnews' THEN 2 WHEN 'sec network' THEN 2 ELSE 3 END"
                )
            order += f", {priority_col} ASC" if priority_col else ""
        else:
            order = f"ORDER BY {priority_col} ASC" if priority_col else ""

        cur.execute(
            f"SELECT {', '.join(select_cols)} FROM playables WHERE {where} {order} LIMIT 1",
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            return empty

        r = dict(row) if isinstance(row, sqlite3.Row) else dict(zip(select_cols, row))
        deeplink = next((r.get(c) for c in ["deeplink_play", "deeplink_open", "playable_url"] if r.get(c)), None)

        espn_id = r.get(espn_col) if espn_col else None
        if is_espn and espn_id and deeplink:
            try:
                playback_id = espn_id.replace("espn-watch:", "", 1)
                if deeplink.startswith("sportscenter://"):
                    deeplink = f"sportscenter://x-callback-url/showWatchStream?playID={playback_id}"
                elif deeplink.startswith("http"):
                    deeplink = f"https://www.espn.com/watch/player/_/id/{playback_id}"
            except Exception:
                pass

        return {
            "deeplink": deeplink,
            "http_deeplink_url": r.get(http_col) if http_col else None,
            "playable_id": r.get(playable_id_col) if playable_id_col else None,
            "espn_graph_id": espn_id,
            "service_name": r.get(svc_name_col) if svc_name_col else None,
        }
    except Exception:
        return empty


def get_playable_id_for_event(conn: sqlite3.Connection, event_id: str, provider_code: str = None) -> Optional[str]:
    """Best-effort lookup of playables.playable_id for an event."""
    if not event_id:
        return None
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(playables)")
        cols = {row[1] for row in cur.fetchall()}
        if "playable_id" not in cols:
            return None

        if provider_code:
            if "logical_service" in cols:
                cur.execute(
                    "SELECT playable_id FROM playables WHERE event_id = ?"
                    " AND playable_id IS NOT NULL AND playable_id != ''"
                    " AND (logical_service = ? OR provider = ?) ORDER BY priority ASC LIMIT 1",
                    (event_id, provider_code, provider_code),
                )
            else:
                cur.execute(
                    "SELECT playable_id FROM playables WHERE event_id = ?"
                    " AND playable_id IS NOT NULL AND playable_id != ''"
                    " AND provider = ? ORDER BY priority ASC LIMIT 1",
                    (event_id, provider_code),
                )
        else:
            cur.execute(
                "SELECT playable_id FROM playables WHERE event_id = ?"
                " AND playable_id IS NOT NULL AND playable_id != ''"
                " ORDER BY priority ASC LIMIT 1",
                (event_id,),
            )
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def get_event_link_info(
    conn: sqlite3.Connection,
    event_id: str,
    uid_col: str,
    primary_deeplink_col: Optional[str],
    full_deeplink_col: Optional[str],
    chosen_provider: Optional[str] = None,
) -> dict:
    """Fetch UID + deeplink info for a given event_id.

    When chosen_provider is set, returns that provider's deeplink directly.
    Otherwise uses filter_integration priority resolution with multiple fallbacks.
    """
    empty = {"event_uid": None, "deeplink_url": None, "deeplink_url_full": None}
    cur = conn.cursor()

    try:
        cur.execute("PRAGMA table_info(events)")
        col_names = {row[1] for row in cur.fetchall()}
    except Exception:
        col_names = set()

    cols = []
    if uid_col and uid_col in col_names:
        cols.append(uid_col)
    if "id" in col_names and "id" not in cols:
        cols.append("id")
    for c in ("pvid", "channel_name", "raw_attributes_json"):
        if c in col_names:
            cols.append(c)
    if primary_deeplink_col and primary_deeplink_col in col_names and primary_deeplink_col not in cols:
        cols.append(primary_deeplink_col)
    if full_deeplink_col and full_deeplink_col in col_names and full_deeplink_col not in cols:
        cols.append(full_deeplink_col)

    if not cols:
        return empty

    try:
        cur.execute(f"SELECT {', '.join(cols)} FROM events WHERE id = ?", (event_id,))
        row = cur.fetchone()
    except Exception:
        return empty
    if not row:
        return empty

    data = dict(zip(cols, row))
    event_uid = data.get(uid_col) if uid_col in data else None
    pvid = data.get("pvid")
    raw_json = data.get("raw_attributes_json")
    primary_value = data.get(primary_deeplink_col) if primary_deeplink_col else None
    full_value = data.get(full_deeplink_col) if full_deeplink_col else None

    deeplink_url = primary_value or full_value

    if chosen_provider and not deeplink_url:
        link = get_provider_playable_link(conn, event_id, chosen_provider)
        dl = link.get("deeplink")
        if dl:
            return {"event_uid": event_uid, "deeplink_url": dl, "deeplink_url_full": dl}

    if not deeplink_url:
        try:
            from filter_integration import (
                load_user_preferences,
                get_best_deeplink_for_event,
                get_fallback_deeplink,
                expand_enabled_services_for_amazon,
            )
            prefs = load_user_preferences(conn)
            enabled = prefs.get("enabled_services", [])
            try:
                enabled = expand_enabled_services_for_amazon(conn, enabled)
            except Exception:
                pass
            try:
                deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled) or None
            except Exception:
                deeplink_url = None
            if not deeplink_url:
                try:
                    event_row = {"id": event_id, "pvid": pvid, "raw_attributes_json": raw_json}
                    deeplink_url = get_fallback_deeplink(event_row) or None
                except Exception:
                    pass
        except ImportError:
            pass

    if not deeplink_url and pvid and not str(event_id).startswith("appletv-"):
        try:
            payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
            deeplink_url = "https://www.peacocktv.com/deeplink?deeplinkData=" + urllib.parse.quote(
                json.dumps(payload, separators=(",", ":"), ensure_ascii=False), safe=""
            )
        except Exception:
            pass

    if not deeplink_url:
        try:
            cur.execute(
                "SELECT playable_url FROM playables WHERE event_id = ?"
                " AND playable_url IS NOT NULL ORDER BY priority ASC LIMIT 1",
                (event_id,),
            )
            prow = cur.fetchone()
            if prow and prow[0]:
                deeplink_url = prow[0]
        except Exception:
            pass

    apple_url = None
    if raw_json:
        try:
            apple_url = json.loads(raw_json).get("apple_tv_url")
        except Exception:
            pass

    if not deeplink_url and apple_url:
        deeplink_url = apple_url

    deeplink_full = full_value or deeplink_url or apple_url
    return {"event_uid": event_uid, "deeplink_url": deeplink_url, "deeplink_url_full": deeplink_full}


def get_current_events_by_lane(conn: sqlite3.Connection, at_ts: Optional[str] = None) -> dict:
    """Return dict of lane_id -> current event row at the given time."""
    if at_ts is None:
        at_ts = _dt.utcnow().isoformat(timespec="seconds")

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT le.lane_id, le.event_id, le.start_utc, le.end_utc,
                   le.is_placeholder, e.title, e.channel_name, e.synopsis
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

    result = {}
    for row in cur.fetchall():
        lane_id = row["lane_id"]
        if lane_id not in result:
            result[lane_id] = dict(row)
    return result


def get_fallback_event_for_lane(conn: sqlite3.Connection, lane_id: int, at_ts: str) -> Optional[dict]:
    """Return the most recent non-placeholder event within the padding window, or None."""
    padding_minutes = int(os.getenv("FRUIT_PADDING_MINUTES", "45"))
    try:
        now_dt = _dt.fromisoformat(at_ts.replace("Z", "+00:00"))
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
    except Exception:
        now_dt = _dt.now(timezone.utc)

    window_start = (now_dt - timedelta(minutes=padding_minutes)).isoformat()

    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT le.event_id, le.start_utc, le.end_utc, le.chosen_provider,
                   e.title, e.channel_name, e.synopsis
            FROM lane_events le
            JOIN events e ON le.event_id = e.id
            WHERE le.lane_id = ?
              AND le.is_placeholder = 0
              AND datetime(le.end_utc) >= datetime(?)
              AND datetime(le.end_utc) <= datetime(?)
            ORDER BY le.end_utc DESC LIMIT 1
            """,
            (lane_id, window_start, at_ts),
        )
        row = cur.fetchone()
    except Exception as e:
        log(f"Error in get_fallback_event_for_lane: {e}", "ERROR")
        return None

    if not row:
        return None
    return {
        "event_id": row["event_id"],
        "title": row["title"],
        "channel_name": row["channel_name"],
        "synopsis": row["synopsis"],
        "start_utc": row["start_utc"],
        "end_utc": row["end_utc"],
        "chosen_provider": row["chosen_provider"],
        "is_fallback": True,
    }


def _apply_http_deeplink_format(conn, event_id, chosen_provider, deeplink_url, deeplink_url_full,
                                 classification_json=None):
    """Convert scheme deeplinks to HTTP format using deeplink_converter. Returns (url, full_url)."""
    try:
        from deeplink_converter import generate_http_deeplink
    except ImportError:
        return deeplink_url, deeplink_url_full

    league_hint = None
    if classification_json:
        try:
            for item in json.loads(classification_json):
                if isinstance(item, dict) and item.get("type") == "league":
                    league_hint = item.get("value")
                    break
        except Exception:
            pass

    def _convert(url):
        if not url:
            return url
        playable_uuid = get_playable_id_for_event(conn, event_id, chosen_provider)
        try:
            http = generate_http_deeplink(
                url, provider=chosen_provider, playable_id=playable_uuid, league=league_hint
            )
        except TypeError:
            http = (
                generate_http_deeplink(url, chosen_provider)
                if chosen_provider
                else generate_http_deeplink(url)
            )
        return http or url

    return _convert(deeplink_url), _convert(deeplink_url_full)


def resolve_whatson(
    conn: sqlite3.Connection,
    lane_id: int,
    at_ts: str,
    want_deeplink: bool = False,
    deeplink_format: str = "scheme",
) -> dict:
    """Core lane status resolver. Returns a dict suitable for building JSON/text responses.

    Keys: ok, lane, event_uid, at, title (optional), deeplink_url (optional),
          deeplink_url_full (optional), is_fallback (optional), event_id (internal).
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT le.event_id, le.start_utc, le.end_utc, le.is_placeholder,
               le.chosen_provider, e.title, e.channel_name, e.synopsis,
               e.classification_json
        FROM lane_events le
        LEFT JOIN events e ON le.event_id = e.id
        WHERE le.lane_id = ?
          AND datetime(le.start_utc) <= datetime(?)
          AND datetime(le.end_utc) > datetime(?)
        ORDER BY le.start_utc DESC LIMIT 1
        """,
        (lane_id, at_ts, at_ts),
    )
    row = cur.fetchone()

    uid_col, primary_col, full_col = get_event_link_columns(conn)

    event_id = None
    event_uid = None
    deeplink_url = None
    deeplink_url_full = None
    is_fallback = False
    title = None
    chosen_provider = None
    classification_json = None

    try:
        chosen_provider = row["chosen_provider"] if row and "chosen_provider" in row.keys() else None
    except Exception:
        pass

    if row and row["is_placeholder"]:
        log(f"Lane {lane_id}: current slot is placeholder, checking fallback", "INFO")
        fallback = get_fallback_event_for_lane(conn, lane_id, at_ts)
        if fallback:
            event_id = fallback["event_id"]
            title = fallback["title"]
            is_fallback = True
            chosen_provider = fallback.get("chosen_provider") or chosen_provider
            link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col, chosen_provider)
            event_uid = link_info["event_uid"]
            deeplink_url = link_info["deeplink_url"]
            deeplink_url_full = link_info["deeplink_url_full"]
    elif row and not row["is_placeholder"]:
        event_id = row["event_id"]
        title = row["title"]
        classification_json = row["classification_json"] if "classification_json" in row.keys() else None
        link_info = get_event_link_info(conn, event_id, uid_col, primary_col, full_col, chosen_provider)
        event_uid = link_info["event_uid"]
        deeplink_url = link_info["deeplink_url"]
        deeplink_url_full = link_info["deeplink_url_full"]

    if deeplink_format == "http" and (deeplink_url or deeplink_url_full):
        deeplink_url, deeplink_url_full = _apply_http_deeplink_format(
            conn, event_id, chosen_provider, deeplink_url, deeplink_url_full, classification_json
        )

    result: dict = {"ok": True, "lane": lane_id, "event_uid": event_uid, "at": at_ts,
                    "_event_id": event_id, "_chosen_provider": chosen_provider}
    if title:
        result["title"] = title
    if want_deeplink:
        result["deeplink_url"] = deeplink_url
        result["deeplink_url_full"] = deeplink_url_full
    if is_fallback:
        result["is_fallback"] = True
    return result
