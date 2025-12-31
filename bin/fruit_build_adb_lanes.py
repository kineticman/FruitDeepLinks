#!/usr/bin/env python3
"""
fruit_build_adb_lanes.py

Builds per-provider ADB lanes into the `adb_lanes` table from `events` + `playables`,
respecting:
- provider_lanes.adb_enabled + provider_lanes.adb_lane_count
- user_preferences: enabled_services, disabled_sports, disabled_leagues

Semantics:
- enabled_services = [] (or missing) means "allow all services".
  We do NOT auto-expand enabled_services to service_priorities keys, because that
  breaks providers not present there (e.g., `kayo_web`).

This script is self-contained (does not import filter_integration) to avoid
preference-default drift.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sqlite3
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Import ADB provider mapping helper
try:
    from adb_provider_mapper import get_logical_services_for_adb_provider
    ADB_MAPPING_AVAILABLE = True
except ImportError:
    ADB_MAPPING_AVAILABLE = False
    def get_logical_services_for_adb_provider(provider: str) -> List[str]:
        return [provider]


UTC = dt.timezone.utc


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("fruit_build_adb_lanes")


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;", (name,))
    return cur.fetchone() is not None


def safe_json_loads(s: str) -> Any:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return None


def load_user_preferences(conn: sqlite3.Connection, log: logging.Logger) -> Dict[str, Any]:
    """
    user_preferences schema:
      user_preferences(key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)

    Values for list keys are stored as JSON arrays.
    """
    prefs: Dict[str, Any] = {
        "enabled_services": [],
        "disabled_sports": [],
        "disabled_leagues": [],
    }

    if not table_exists(conn, "user_preferences"):
        log.info("No user_preferences table found; using defaults (allow all services).")
        return prefs

    cur = conn.cursor()
    try:
        cur.execute("SELECT key, value FROM user_preferences;")
        rows = cur.fetchall()
    except Exception as e:
        log.warning("Failed reading user_preferences (using defaults): %s", e)
        return prefs

    raw: Dict[str, str] = {k: (v or "") for (k, v) in rows}

    def get_list(key: str) -> List[str]:
        v = raw.get(key)
        if v is None:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            s = v.strip()
            # If it's a JSON list, use it.
            arr = safe_json_loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
            # Fall back to comma-separated, but guard against accidentally storing a JSON object string.
            parts = [x.strip() for x in s.split(",") if x.strip()]
            # Drop obvious JSON-object fragments like '{"a": 1' or '"a": 1}' so we don't
            # accidentally treat a dict string as an enabled_services allowlist.
            cleaned: List[str] = []
            token_re = re.compile(r"^[A-Za-z0-9._-]+$")  # provider_code / logical_service-like
            for p in parts:
                if "{" in p or "}" in p or ":" in p:
                    continue
                if token_re.match(p):
                    cleaned.append(p)
            return cleaned
        return []
        return []

    prefs["enabled_services"] = get_list("enabled_services")
    prefs["disabled_sports"] = get_list("disabled_sports")
    prefs["disabled_leagues"] = get_list("disabled_leagues")

    log.info(
        "ADB filters loaded: enabled_services=%s disabled_sports=%d disabled_leagues=%d",
        ("ALL" if not prefs["enabled_services"] else str(len(prefs["enabled_services"]))),
        len(prefs["disabled_sports"]),
        len(prefs["disabled_leagues"]),
    )
    return prefs


def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(UTC)
        return dt.datetime.fromisoformat(s).astimezone(UTC)
    except Exception:
        return None


def ms_to_dt(ms: Any) -> Optional[dt.datetime]:
    try:
        if ms is None:
            return None
        return dt.datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC)
    except Exception:
        return None


def dt_to_iso(d: dt.datetime) -> str:
    d = d.astimezone(UTC).replace(microsecond=0)
    return d.isoformat()  # ...+00:00


def should_include_event(classification_json: str, disabled_sports: Sequence[str], disabled_leagues: Sequence[str]) -> bool:
    if not disabled_sports and not disabled_leagues:
        return True

    cj = classification_json or ""
    parsed = safe_json_loads(cj)

    sport_vals: List[str] = []
    league_vals: List[str] = []

    if isinstance(parsed, list):
        for obj in parsed:
            if isinstance(obj, dict):
                t = str(obj.get("type", "")).strip().lower()
                v = str(obj.get("value", "")).strip()
                if not v:
                    continue
                if t == "sport":
                    sport_vals.append(v)
                elif t == "league":
                    league_vals.append(v)

    def norm(x: str) -> str:
        return (x or "").strip().lower()

    ds = {norm(x) for x in disabled_sports if norm(x)}
    dl = {norm(x) for x in disabled_leagues if norm(x)}

    if sport_vals and any(norm(v) in ds for v in sport_vals):
        return False
    if league_vals and any(norm(v) in dl for v in league_vals):
        return False

    raw_low = cj.lower()
    if any(x and x in raw_low for x in ds):
        return False
    if any(x and x in raw_low for x in dl):
        return False

    return True


def load_adb_enabled_providers(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT provider_code, COALESCE(adb_lane_count, 0) AS lanes
        FROM provider_lanes
        WHERE COALESCE(adb_enabled, 0) = 1
          AND COALESCE(adb_lane_count, 0) > 0
        ORDER BY provider_code
        """
    )
    out: List[Tuple[str, int]] = []
    for code, lanes in cur.fetchall():
        try:
            out.append((str(code), int(lanes)))
        except Exception:
            continue
    return out


def clear_adb_lanes(conn: sqlite3.Connection, provider_code: str, log: logging.Logger) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM adb_lanes WHERE provider_code=?;", (provider_code,))
    conn.commit()
    log.info("Cleared adb_lanes for provider %s", provider_code)


def load_events_for_provider(
    conn: sqlite3.Connection, 
    provider_code: str,
    enabled_services: List[str]
) -> List[Dict[str, Any]]:
    """
    Load events for an ADB provider code.
    
    Note: provider_code may map to multiple logical_service values.
    For example, 'sportscenter' maps to both 'espn_linear' and 'espn_plus'.
    Only events with playables from ENABLED logical services are included.
    """
    cur = conn.cursor()
    
    # Get all logical services that map to this ADB provider
    all_logical_services = get_logical_services_for_adb_provider(provider_code)
    
    # Filter to only include enabled services
    # If enabled_services is empty, include all (legacy behavior)
    if enabled_services:
        logical_services = [ls for ls in all_logical_services if ls in enabled_services]
    else:
        logical_services = all_logical_services
    
    if not logical_services:
        # No enabled services for this provider
        return []
    
    # Build query with IN clause for multiple services
    placeholders = ','.join('?' * len(logical_services))
    query = f"""
        SELECT
            e.id,
            e.title,
            e.start_utc,
            e.end_utc,
            e.start_ms,
            e.end_ms,
            e.classification_json
        FROM events e
        JOIN playables p ON p.event_id = e.id
        WHERE p.logical_service IN ({placeholders})
        GROUP BY e.id
    """
    
    cur.execute(query, logical_services)
    
    out: List[Dict[str, Any]] = []
    for (eid, title, start_utc, end_utc, start_ms, end_ms, classification_json) in cur.fetchall():
        out.append(
            {
                "id": eid,
                "title": title or "",
                "start_utc": start_utc or "",
                "end_utc": end_utc or "",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "classification_json": classification_json or "",
            }
        )
    return out


def assign_to_lanes(
    events: Sequence[Dict[str, Any]],
    lane_count: int,
) -> List[Tuple[int, Dict[str, Any], dt.datetime, dt.datetime]]:
    lane_ends: List[dt.datetime] = [dt.datetime.min.replace(tzinfo=UTC) for _ in range(lane_count)]
    assignments: List[Tuple[int, Dict[str, Any], dt.datetime, dt.datetime]] = []

    def start_dt(ev: Dict[str, Any]) -> dt.datetime:
        st = parse_iso_utc(ev.get("start_utc", "")) or ms_to_dt(ev.get("start_ms"))
        return st or dt.datetime.max.replace(tzinfo=UTC)

    for ev in sorted(events, key=start_dt):
        st = parse_iso_utc(ev.get("start_utc", "")) or ms_to_dt(ev.get("start_ms"))
        en = parse_iso_utc(ev.get("end_utc", "")) or ms_to_dt(ev.get("end_ms"))
        if not st or not en or en <= st:
            continue

        best_lane = None
        best_end = None
        for i, lane_end in enumerate(lane_ends):
            if lane_end <= st:
                if best_end is None or lane_end < best_end:
                    best_end = lane_end
                    best_lane = i

        if best_lane is None:
            continue

        lane_ends[best_lane] = en
        assignments.append((best_lane + 1, ev, st, en))

    return assignments


def insert_adb_rows(
    conn: sqlite3.Connection,
    provider_code: str,
    assignments: Sequence[Tuple[int, Dict[str, Any], dt.datetime, dt.datetime]],
) -> int:
    cur = conn.cursor()
    n = 0
    for lane_number, ev, st, en in assignments:
        channel_id = f"{provider_code}{lane_number:02d}"
        cur.execute(
            """
            INSERT INTO adb_lanes (provider_code, lane_number, channel_id, event_id, start_utc, stop_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (provider_code, lane_number, channel_id, ev["id"], dt_to_iso(st), dt_to_iso(en)),
        )
        n += 1
    conn.commit()
    return n


def build_adb_lanes(db_path: str, provider_filter: Optional[str] = None) -> None:
    log = setup_logging()
    log.info("Using database: %s", db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    if not table_exists(conn, "adb_lanes"):
        raise RuntimeError("adb_lanes table not found. Run migrate_add_adb_lanes.py first.")
    if not table_exists(conn, "provider_lanes"):
        raise RuntimeError("provider_lanes table not found. Run migrate_add_provider_lanes.py first.")

    prefs = load_user_preferences(conn, log)
    enabled_services: List[str] = prefs.get("enabled_services") or []
    disabled_sports: List[str] = prefs.get("disabled_sports") or []
    disabled_leagues: List[str] = prefs.get("disabled_leagues") or []

    providers = load_adb_enabled_providers(conn)
    if provider_filter:
        providers = [(c, n) for (c, n) in providers if c == provider_filter]

    if not providers:
        log.info("No ADB-enabled providers to build (provider_filter=%s).", provider_filter or "None")
        return

    log.info(
        "Loaded %d ADB-enabled provider(s): %s",
        len(providers),
        ", ".join([f"{c} ({n} lanes)" for (c, n) in providers]),
    )

    total_inserted = 0

    for provider_code, lane_count in providers:
        clear_adb_lanes(conn, provider_code, log)

        # Only enforce enabled_services when the user explicitly set a non-empty allowlist.
        # Check if ANY of the logical services mapped to this ADB provider are enabled
        if enabled_services:
            logical_services = get_logical_services_for_adb_provider(provider_code)
            if not any(ls in enabled_services for ls in logical_services):
                log.info("Skipping provider %s because none of its logical services %s are in enabled_services", 
                        provider_code, logical_services)
                continue

        evs = load_events_for_provider(conn, provider_code, enabled_services)

        filtered: List[Dict[str, Any]] = []
        null_ts = 0
        for ev in evs:
            st = parse_iso_utc(ev.get("start_utc", "")) or ms_to_dt(ev.get("start_ms"))
            en = parse_iso_utc(ev.get("end_utc", "")) or ms_to_dt(ev.get("end_ms"))
            if not st or not en:
                null_ts += 1
                continue
            if not should_include_event(ev.get("classification_json", ""), disabled_sports, disabled_leagues):
                continue
            filtered.append(ev)

        if null_ts:
            log.warning(
                "Provider %s: filtered out %d events with null timestamps (keeping %d valid events)",
                provider_code,
                null_ts,
                len(filtered),
            )

        if not filtered:
            log.info("Provider %s: no events after filtering; nothing to insert.", provider_code)
            continue

        assignments = assign_to_lanes(filtered, lane_count)
        inserted = insert_adb_rows(conn, provider_code, assignments)
        total_inserted += inserted
        log.info("Provider %s: inserted %d adb_lanes rows", provider_code, inserted)

    log.info("ADB lane build complete. Total adb_lanes rows inserted: %d", total_inserted)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", "--db-path", dest="db_path", default="/app/data/fruit_events.db", help="SQLite DB path")
    ap.add_argument("--provider", dest="provider_filter", default=None, help="Build only a single provider_code")
    args = ap.parse_args()
    build_adb_lanes(args.db_path, provider_filter=args.provider_filter)


if __name__ == "__main__":
    main()