#!/usr/bin/env python3
"""
server/services/filters.py - Filter and preference business logic

Thin service layer over db.preferences and filter_integration.
All DB access goes through get_conn() from db.connection.
"""

import json
import sqlite3
from typing import Any, Dict, List

from db.connection import get_conn, get_conn_or_none, db_exists
from db import preferences as prefs_dal

# Optional dependencies — import gracefully so the service can load even if
# individual helper modules haven't been migrated yet.
try:
    from logical_service_mapper import get_all_logical_services_with_counts
    from core.service_catalog import get_display_name, get_canonical_service_code
    _LOGICAL_SERVICES_AVAILABLE = True
except ImportError:
    _LOGICAL_SERVICES_AVAILABLE = False

    def get_canonical_service_code(service_code):
        return service_code

try:
    from filter_integration import expand_enabled_services_for_amazon
    _FILTER_AVAILABLE = True
except ImportError:
    _FILTER_AVAILABLE = False

try:
    from provider_utils import get_provider_display_name
    _PROVIDER_UTILS_AVAILABLE = True
except ImportError:
    _PROVIDER_UTILS_AVAILABLE = False


def get_preferences() -> Dict[str, Any]:
    """Return user preferences merged with defaults. Never raises."""
    if not db_exists():
        return prefs_dal._DEFAULTS.copy()
    try:
        with get_conn() as conn:
            return prefs_dal.load(conn)
    except Exception:
        return prefs_dal._DEFAULTS.copy()


def save_preferences(prefs: Dict[str, Any]) -> bool:
    """Persist user preferences. Returns True on success."""
    if not db_exists():
        return False
    try:
        with get_conn() as conn:
            return prefs_dal.save(conn, prefs)
    except Exception:
        return False


def get_auto_refresh() -> Dict[str, Any]:
    """Return auto-refresh settings (enabled + time)."""
    if not db_exists():
        from server.config import cfg
        return {"enabled": cfg.AUTO_REFRESH_ENABLED, "time": cfg.AUTO_REFRESH_TIME}
    try:
        with get_conn() as conn:
            return prefs_dal.load_auto_refresh(conn)
    except Exception:
        from server.config import cfg
        return {"enabled": cfg.AUTO_REFRESH_ENABLED, "time": cfg.AUTO_REFRESH_TIME}


def save_auto_refresh(settings: Dict[str, Any]) -> bool:
    """Persist auto-refresh settings."""
    if not db_exists():
        return False
    try:
        with get_conn() as conn:
            return prefs_dal.save_auto_refresh(conn, settings)
    except Exception:
        return False


def get_available_filters() -> Dict[str, Any]:
    """
    Return { providers, amazon_services, espn_services, sports, leagues } for the
    Filters UI. Providers, Amazon services, and ESPN services are split so the
    UI can show each group separately.
    """
    empty = {
        "providers": [], "amazon_services": [], "espn_services": [],
        "sports": [], "leagues": [], "teams": [],
    }
    if not db_exists():
        return empty

    try:
        with get_conn() as conn:
            return _build_filters(conn)
    except Exception:
        return empty


def _build_filters(conn: sqlite3.Connection) -> Dict[str, Any]:
    providers: List[dict] = []
    amazon_services: List[dict] = []
    espn_services: List[dict] = []

    if _LOGICAL_SERVICES_AVAILABLE:
        try:
            raw_counts = get_all_logical_services_with_counts(conn)
            # Normalize legacy aliases (e.g. old playables tagged
            # 'aiv_watch_for_free') to their canonical code and merge counts
            # *before* building checkbox entries. Without this, a raw legacy
            # code shows as its own separate, permanently-un-toggleable
            # checkbox: saving normalizes enabled_services to the canonical
            # code (db/preferences.py), so the legacy-keyed checkbox can never
            # match on reload and always renders disabled again, even after
            # being checked and saved.
            service_counts: Dict[str, int] = {}
            for code, count in raw_counts.items():
                canonical = get_canonical_service_code(code)
                service_counts[canonical] = service_counts.get(canonical, 0) + count

            for code, count in sorted(service_counts.items(), key=lambda x: -x[1]):
                entry = {"scheme": code, "name": get_display_name(code), "count": count}
                if code == "aiv" or code.startswith("aiv_"):
                    amazon_services.append(entry)
                elif code == "sportscenter" or code.startswith("espn"):
                    espn_services.append(entry)
                else:
                    providers.append(entry)
        except Exception:
            pass
    else:
        # Fallback: raw provider grouping
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT provider, COUNT(*) AS count
                FROM playables
                WHERE provider IS NOT NULL AND provider != ''
                GROUP BY provider
                ORDER BY count DESC
                """
            )
            provider_counts: Dict[str, int] = {}
            for row in cur.fetchall():
                provider = row[0] if not hasattr(row, "__getitem__") else row["provider"]
                count = row[1] if not hasattr(row, "__getitem__") else row["count"]
                canonical = get_canonical_service_code(provider)
                provider_counts[canonical] = provider_counts.get(canonical, 0) + count

            for provider, count in sorted(provider_counts.items(), key=lambda x: -x[1]):
                name = get_display_name(provider) if _LOGICAL_SERVICES_AVAILABLE else provider.upper()
                entry = {"scheme": provider, "name": name, "count": count}
                if provider == "aiv":
                    amazon_services.append(entry)
                elif provider == "sportscenter":
                    espn_services.append(entry)
                else:
                    providers.append(entry)
        except Exception:
            pass

    # Sports from genres_json
    sports: Dict[str, int] = {}
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT genres_json, COUNT(*) AS event_count
            FROM events
            WHERE datetime(end_utc) > datetime('now')
              AND genres_json IS NOT NULL AND genres_json != '[]'
            GROUP BY genres_json
            """
        )
        for row in cur.fetchall():
            gj = row[0] if not isinstance(row, sqlite3.Row) else row["genres_json"]
            ec = row[1] if not isinstance(row, sqlite3.Row) else row["event_count"]
            try:
                for genre in json.loads(gj):
                    if genre and isinstance(genre, str):
                        sports[genre] = sports.get(genre, 0) + ec
            except Exception:
                pass
    except Exception:
        pass

    sports_list = [{"name": k, "count": v} for k, v in sorted(sports.items(), key=lambda x: -x[1])]

    leagues_list = _build_active_leagues(conn)

    teams_list = _build_active_teams(conn)

    return {
        "providers": providers,
        "amazon_services": amazon_services,
        "espn_services": espn_services,
        "sports": sports_list,
        "leagues": leagues_list,
        "teams": teams_list,
    }


def _build_active_leagues(conn: sqlite3.Connection) -> List[dict]:
    """Return active leagues with the sports represented by their events."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT classification_json, genres_json, COUNT(*) AS event_count
            FROM events
            WHERE datetime(end_utc) > datetime('now')
              AND classification_json IS NOT NULL
              AND classification_json != '[]'
            GROUP BY classification_json, genres_json
            """
        )
    except Exception:
        return []

    leagues: Dict[str, dict] = {}
    for row in cur.fetchall():
        classification_json = row[0] if not isinstance(row, sqlite3.Row) else row["classification_json"]
        genres_json = row[1] if not isinstance(row, sqlite3.Row) else row["genres_json"]
        event_count = row[2] if not isinstance(row, sqlite3.Row) else row["event_count"]
        try:
            classifications = json.loads(classification_json)
            genres = json.loads(genres_json or "[]")
        except Exception:
            continue

        sports = {
            genre for genre in genres
            if isinstance(genre, str) and genre
        } if isinstance(genres, list) else set()
        for item in classifications if isinstance(classifications, list) else []:
            if not isinstance(item, dict) or item.get("type") != "league":
                continue
            name = item.get("value")
            if not name:
                continue
            entry = leagues.setdefault(name, {"name": name, "count": 0, "sports": set()})
            entry["count"] += event_count
            entry["sports"].update(sports)

    return [
        {"name": entry["name"], "count": entry["count"], "sports": sorted(entry["sports"])}
        for entry in sorted(leagues.values(), key=lambda item: (-item["count"], item["name"].casefold()))
    ]


def _build_active_teams(conn: sqlite3.Connection) -> List[dict]:
    """Extract exact team identities from structured data on active events.

    This deliberately does not infer teams from titles or nicknames.  A title
    search for "Bulls" would mix Chicago, Buffalo, and South Florida, while
    Apple's competitor objects provide full names and stable IDs.
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(events)")
        columns = {row[1] for row in cur.fetchall()}
        if "raw_attributes_json" not in columns:
            return []
        cur.execute(
            """
            SELECT id, raw_attributes_json, genres_json, classification_json
            FROM events
            WHERE datetime(end_utc) > datetime('now')
              AND raw_attributes_json IS NOT NULL
              AND raw_attributes_json != ''
            """
        )
    except Exception:
        return []

    teams: Dict[str, dict] = {}
    for row in cur.fetchall():
        raw_json = row[1] if not isinstance(row, sqlite3.Row) else row["raw_attributes_json"]
        genres_json = row[2] if not isinstance(row, sqlite3.Row) else row["genres_json"]
        class_json = row[3] if not isinstance(row, sqlite3.Row) else row["classification_json"]
        try:
            attrs = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        except Exception:
            continue
        if not isinstance(attrs, dict):
            continue

        sport = attrs.get("sport_name") or ""
        league = attrs.get("league_name") or ""
        if not sport:
            try:
                genres = json.loads(genres_json or "[]")
                sport = next((value for value in genres if isinstance(value, str) and value), "")
            except Exception:
                pass
        if not league:
            try:
                classifications = json.loads(class_json or "[]")
                league = next(
                    (item.get("value") for item in classifications
                     if isinstance(item, dict) and item.get("type") == "league" and item.get("value")),
                    "",
                )
            except Exception:
                pass
        if not sport or not league:
            continue

        competitors = attrs.get("competitors") or []
        if not isinstance(competitors, list):
            continue
        seen_for_event = set()
        for competitor in competitors:
            if not isinstance(competitor, dict):
                continue
            if str(competitor.get("type") or "Team").casefold() != "team":
                continue
            name = str(competitor.get("name") or "").strip()
            team_id = str(competitor.get("id") or "").strip()
            if not name:
                continue
            identity = team_id or f"{sport.casefold()}|{league.casefold()}|{name.casefold()}"
            if identity in seen_for_event:
                continue
            seen_for_event.add(identity)
            entry = teams.setdefault(identity, {
                "team_id": team_id,
                "name": name,
                "sport": str(sport),
                "league": str(league),
                "count": 0,
            })
            entry["count"] += 1

    return sorted(
        teams.values(),
        key=lambda item: (item["sport"].casefold(), item["league"].casefold(), item["name"].casefold()),
    )


def expand_amazon(enabled_services: List[str]) -> List[str]:
    """Expand 'aiv' master to concrete sub-service codes."""
    if not _FILTER_AVAILABLE or not db_exists():
        return enabled_services
    try:
        with get_conn() as conn:
            return expand_enabled_services_for_amazon(conn, enabled_services)
    except Exception:
        return enabled_services


def clear_stale_services() -> Dict[str, Any]:
    """Remove enabled services that have no future events."""
    if not db_exists():
        return {"status": "error", "message": "Database not available"}
    try:
        with get_conn() as conn:
            return _do_clear_stale(conn)
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _do_clear_stale(conn: sqlite3.Connection) -> Dict[str, Any]:
    prefs = prefs_dal.load(conn)
    enabled = prefs.get("enabled_services", [])
    if not enabled:
        return {"status": "ok", "removed": [], "message": "No enabled services to check"}

    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT p.logical_service
        FROM playables p
        JOIN events e ON p.event_id = e.id
        WHERE datetime(e.end_utc) > datetime('now')
          AND p.logical_service IS NOT NULL AND p.logical_service != ''
        """
    )
    active = {row[0] for row in cur.fetchall()}
    if any(s.startswith("aiv") for s in active):
        active.add("aiv")

    if not prefs.get("amazon_master_enabled", True):
        active = {s for s in active if s != "aiv" and not s.startswith("aiv_")}

    stale = [s for s in enabled if s not in active]
    if not stale:
        return {"status": "ok", "removed": [], "message": "No stale services found"}

    prefs["enabled_services"] = [s for s in enabled if s in active]
    if prefs_dal.save(conn, prefs):
        return {"status": "ok", "removed": stale, "kept": prefs["enabled_services"]}
    return {"status": "error", "message": "Failed to save preferences"}
