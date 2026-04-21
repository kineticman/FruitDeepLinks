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
    from core.service_catalog import get_display_name
    _LOGICAL_SERVICES_AVAILABLE = True
except ImportError:
    _LOGICAL_SERVICES_AVAILABLE = False

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
    Return { providers, amazon_services, sports, leagues } for the Filters UI.
    Providers and Amazon services are split so the UI can show them separately.
    """
    empty = {"providers": [], "amazon_services": [], "sports": [], "leagues": []}
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

    if _LOGICAL_SERVICES_AVAILABLE:
        try:
            service_counts = get_all_logical_services_with_counts(conn)
            for code, count in sorted(service_counts.items(), key=lambda x: -x[1]):
                entry = {"scheme": code, "name": get_display_name(code), "count": count}
                if code == "aiv" or code.startswith("aiv_"):
                    amazon_services.append(entry)
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
            for row in cur.fetchall():
                provider = row[0] if not hasattr(row, "__getitem__") else row["provider"]
                count = row[1] if not hasattr(row, "__getitem__") else row["count"]
                name = get_display_name(provider) if _LOGICAL_SERVICES_AVAILABLE else provider.upper()
                entry = {"scheme": provider, "name": name, "count": count}
                if provider == "aiv":
                    amazon_services.append(entry)
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
            WHERE end_utc > datetime('now')
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

    # Leagues from classification_json
    leagues: Dict[str, int] = {}
    try:
        cur.execute(
            """
            SELECT classification_json, COUNT(*) AS event_count
            FROM events
            WHERE end_utc > datetime('now')
              AND classification_json IS NOT NULL AND classification_json != '[]'
            GROUP BY classification_json
            """
        )
        for row in cur.fetchall():
            cj = row[0] if not isinstance(row, sqlite3.Row) else row["classification_json"]
            ec = row[1] if not isinstance(row, sqlite3.Row) else row["event_count"]
            try:
                for item in json.loads(cj):
                    if isinstance(item, dict) and item.get("type") == "league":
                        name = item.get("value")
                        if name:
                            leagues[name] = leagues.get(name, 0) + ec
            except Exception:
                pass
    except Exception:
        pass

    leagues_list = [
        {"name": k, "count": v}
        for k, v in sorted(leagues.items(), key=lambda x: -x[1])[:50]
    ]

    return {
        "providers": providers,
        "amazon_services": amazon_services,
        "sports": sports_list,
        "leagues": leagues_list,
    }


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
