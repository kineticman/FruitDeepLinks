#!/usr/bin/env python3
"""
fruit_build_adb_lanes.py

Builds per-provider ADB lanes into the `adb_lanes` table from `events` + `playables`,
respecting:
- provider_lanes.adb_enabled + provider_lanes.adb_lane_count
- user_preferences: enabled_services, disabled_sports, disabled_leagues, team_rules

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

# Settings (distinct from filter preferences) — Settings page "Pipeline" section
try:
    from db.preferences import get_setting
except ImportError:
    def get_setting(conn, key, fallback=None):
        return fallback

try:
    from core.service_catalog import get_canonical_service_code, expand_with_legacy_aliases
except ImportError:
    def get_canonical_service_code(service_code):
        return service_code

    def expand_with_legacy_aliases(service_codes):
        return list(service_codes)

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
        "team_rules": [],
        "language_preference": "en",
        "amazon_master_enabled": True,
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

    prefs["enabled_services"] = [
        get_canonical_service_code(service)
        for service in get_list("enabled_services")
    ]
    prefs["disabled_sports"] = get_list("disabled_sports")
    prefs["disabled_leagues"] = get_list("disabled_leagues")
    team_rules_raw = safe_json_loads(raw.get("team_rules", ""))
    prefs["team_rules"] = team_rules_raw if isinstance(team_rules_raw, list) else []

    lang_raw = raw.get("language_preference")
    if lang_raw:
        lang = safe_json_loads(lang_raw) if isinstance(lang_raw, str) else lang_raw
        if isinstance(lang, str) and lang in ("en", "es", "both"):
            prefs["language_preference"] = lang

    amazon_master_raw = raw.get("amazon_master_enabled")
    if amazon_master_raw is not None:
        parsed = safe_json_loads(amazon_master_raw) if isinstance(amazon_master_raw, str) else amazon_master_raw
        if isinstance(parsed, bool):
            prefs["amazon_master_enabled"] = parsed

    log.info(
        "ADB filters loaded: enabled_services=%s disabled_sports=%d disabled_leagues=%d team_rules=%d",
        ("ALL" if not prefs["enabled_services"] else str(len(prefs["enabled_services"]))),
        len(prefs["disabled_sports"]),
        len(prefs["disabled_leagues"]),
        len(prefs["team_rules"]),
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


def _norm_filter_value(value: Any) -> str:
    return str(value or "").strip().casefold()


def _passes_team_rules(raw_attributes_json: str, classification_json: str, team_rules: Sequence[Any]) -> bool:
    if not team_rules:
        return True
    attrs = safe_json_loads(raw_attributes_json or "")
    if not isinstance(attrs, dict):
        attrs = {}
    competitors = attrs.get("competitors") or []
    if not isinstance(competitors, list):
        competitors = []

    sport = _norm_filter_value(attrs.get("sport_name"))
    league = _norm_filter_value(attrs.get("league_name"))
    if not sport or not league:
        parsed = safe_json_loads(classification_json or "")
        if isinstance(parsed, list):
            for item in parsed:
                if not isinstance(item, dict):
                    continue
                kind = _norm_filter_value(item.get("type"))
                value = _norm_filter_value(item.get("value"))
                if kind == "sport" and not sport:
                    sport = value
                elif kind == "league" and not league:
                    league = value

    for rule in team_rules:
        if not isinstance(rule, dict):
            continue
        if _norm_filter_value(rule.get("sport")) != sport or _norm_filter_value(rule.get("league")) != league:
            continue
        mode = _norm_filter_value(rule.get("mode"))
        if mode not in ("include", "exclude"):
            continue
        team_competitors = [
            item for item in competitors
            if isinstance(item, dict) and _norm_filter_value(item.get("type")) in ("", "team")
        ]
        if not team_competitors:
            return bool(rule.get("include_unassigned", True))
        selected_teams = [item for item in (rule.get("teams") or []) if isinstance(item, dict)]
        matched = False
        for competitor in team_competitors:
            for selected in selected_teams:
                selected_id = _norm_filter_value(selected.get("team_id") or selected.get("id"))
                competitor_id = _norm_filter_value(competitor.get("id"))
                if selected_id and competitor_id:
                    if selected_id == competitor_id:
                        matched = True
                        break
                elif (
                    _norm_filter_value(selected.get("name"))
                    and _norm_filter_value(selected.get("name")) == _norm_filter_value(competitor.get("name"))
                ):
                    matched = True
                    break
            if matched:
                break
        return matched if mode == "include" else not matched
    return True


def should_include_event(
    classification_json: str,
    disabled_sports: Sequence[str],
    disabled_leagues: Sequence[str],
    raw_attributes_json: str = "",
    team_rules: Sequence[Any] = (),
) -> bool:
    if not disabled_sports and not disabled_leagues and not team_rules:
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

    if not _passes_team_rules(raw_attributes_json, classification_json, team_rules):
        return False

    # Don't do raw substring matching - it's too aggressive and filters out
    # events where the disabled term appears anywhere in the JSON
    # (e.g., "Basketball" in "Basketball Association")
    # This now matches the behavior of filter_integration.py used by direct output

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
    enabled_services: List[str],
    expand_all_playables: bool = False,
    language_preference: str = "en",
) -> List[Dict[str, Any]]:
    """
    Load events for an ADB provider code.

    Notes:
      - provider_code may map to multiple logical_service values.
      - For Amazon, we intentionally *collapse* all Amazon logical services (aiv_*)
        into the single ADB provider_code "aiv", while still respecting the user's
        enabled_services allowlist.

        Rules for Amazon (provider_code == "aiv"):
          * enabled_services empty  -> include ALL amazon logical_service LIKE 'aiv%%'
          * 'aiv' in enabled_services -> include ALL amazon logical_service LIKE 'aiv%%'
          * otherwise -> include only amazon logical_services explicitly enabled (aiv_*)

    When expand_all_playables is True, one row per matching PLAYABLE is returned
    instead of one row per event (GROUP BY e.id collapsed to a single row) --
    each carries its own "playable_id" so assign_to_lanes() schedules every
    sibling into its own lane instead of just tracking "this event has a
    playable for this provider". Playables that would be excluded by the
    language filter (Spanish feeds under language_preference="en", etc.) are
    still dropped here so lanes aren't burned on feeds that could never be
    selected -- see filter_integration._classify_espn_locale for the same
    check used by direct export and by tune-time selection.
    """
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(events)")
    event_columns = {row[1] for row in cur.fetchall()}
    raw_attributes_select = "e.raw_attributes_json" if "raw_attributes_json" in event_columns else "''"
    select_cols = (
        "e.id, e.title, e.start_utc, e.end_utc, e.start_ms, e.end_ms, "
        f"e.classification_json, {raw_attributes_select} AS raw_attributes_json"
    )
    if expand_all_playables:
        select_cols += ", p.playable_id, p.service_name, p.locale, p.title AS p_title, p.priority, p.logical_service, p.locale_fallback, p.feed_name"

    # --- Amazon special case: collapse all aiv_* services into provider_code "aiv" for ADB ---
    if provider_code == "aiv":
        if not enabled_services or ("aiv" in enabled_services):
            # Include everything Amazon-y, including aiv_aggregator, aiv_vix_premium, etc.
            where = "p.logical_service LIKE 'aiv%'"
            params: List[str] = []
        else:
            allowed = [s for s in enabled_services if isinstance(s, str) and s.startswith("aiv_")]
            if not allowed:
                return []
            # Also match any legacy alias of an allowed code (e.g. a playable
            # still tagged 'aiv_watch_for_free' when 'aiv_free' is enabled) --
            # this compares the raw DB value directly, so it can't go through
            # get_canonical_service_code() per-row without a SQL CASE.
            allowed = expand_with_legacy_aliases(allowed)
            placeholders = ",".join("?" * len(allowed))
            where = f"p.logical_service IN ({placeholders})"
            params = allowed

        group_or_order = "GROUP BY e.id" if not expand_all_playables else "ORDER BY e.start_utc ASC, p.priority ASC"
        query = f"""
            SELECT {select_cols}
            FROM events e
            JOIN playables p ON p.event_id = e.id
            WHERE {where}
            {group_or_order}
        """
        cur.execute(query, params)

    else:
        # Get all logical services that map to this ADB provider
        all_logical_services = get_logical_services_for_adb_provider(provider_code)

        # Filter to only include enabled services
        # If enabled_services is empty, include all (legacy behavior)
        if enabled_services:
            # NOTE: espn_mlb_tv/espn_mlb_network are NOT wildcarded in under
            # 'espn_unlimited' -- see filter_integration.py's
            # expand_enabled_services_for_espn_unlimited() docstring for why
            # that was removed from live filtering (it silently overrode an
            # explicit uncheck). migrate_backfill_espn_unlimited_granular_tiers.py
            # backfills existing saved preferences once instead.
            logical_services = [ls for ls in all_logical_services if ls in enabled_services]
        else:
            logical_services = all_logical_services

        if not logical_services:
            # No enabled services for this provider
            return []

        # Also match any legacy alias of a matched code -- see the aiv branch
        # above for why (raw DB value comparison, no per-row normalization).
        logical_services = expand_with_legacy_aliases(logical_services)

        placeholders = ",".join("?" * len(logical_services))
        group_or_order = "GROUP BY e.id" if not expand_all_playables else "ORDER BY e.start_utc ASC, p.priority ASC"
        query = f"""
            SELECT {select_cols}
            FROM events e
            JOIN playables p ON p.event_id = e.id
            WHERE p.logical_service IN ({placeholders})
            {group_or_order}
        """
        cur.execute(query, logical_services)

    out: List[Dict[str, Any]] = []
    if expand_all_playables:
        try:
            from filter_integration import _classify_espn_locale
        except ImportError:
            def _classify_espn_locale(playable):
                return False, False

        try:
            from xmltv_helpers import label_playables_for_expand
        except ImportError:
            def label_playables_for_expand(playables, fallback, event_title=None):
                return [(p, p.get("service_name") or fallback, fallback) for p in playables]

        # First pass: apply the language filter and stash the base event fields
        # + this row's playable dict (for labeling) keyed by event id, so
        # siblings of the same event can be labeled/disambiguated together
        # below rather than one at a time.
        by_event: Dict[str, Dict[str, Any]] = {}
        playables_by_event: Dict[str, List[Dict[str, Any]]] = {}
        for row in cur.fetchall():
            eid, title, start_utc, end_utc, start_ms, end_ms, classification_json, raw_attributes_json, playable_id, service_name, locale, p_title, priority, logical_service, locale_fallback, feed_name = row
            if language_preference != "both":
                is_spanish, _ = _classify_espn_locale(
                    {"service_name": service_name, "locale": locale, "title": p_title, "locale_fallback": locale_fallback}
                )
                if language_preference == "en" and is_spanish:
                    continue
                if language_preference == "es" and not is_spanish:
                    continue
            by_event[eid] = {
                "id": eid,
                "title": title or "",
                "start_utc": start_utc or "",
                "end_utc": end_utc or "",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "classification_json": classification_json or "",
                "raw_attributes_json": raw_attributes_json or "",
            }
            playables_by_event.setdefault(eid, []).append(
                {
                    "playable_id": playable_id, "service_name": service_name,
                    "logical_service": logical_service, "title": p_title, "feed_name": feed_name,
                }
            )

        try:
            from logical_service_mapper import get_service_display_name as _gsdn
            fallback_label = _gsdn(provider_code) or provider_code.upper()
        except Exception:
            fallback_label = provider_code.upper()
        for eid, siblings in playables_by_event.items():
            event_title = by_event[eid]["title"]
            for playable, label, _group_label in label_playables_for_expand(siblings, fallback=fallback_label, event_title=event_title):
                out.append({**by_event[eid], "playable_id": playable["playable_id"], "playable_label": label})
        return out

    for (eid, title, start_utc, end_utc, start_ms, end_ms, classification_json, raw_attributes_json) in cur.fetchall():
        out.append(
            {
                "id": eid,
                "title": title or "",
                "start_utc": start_utc or "",
                "end_utc": end_utc or "",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "classification_json": classification_json or "",
                "raw_attributes_json": raw_attributes_json or "",
            }
        )
    return out



def assign_to_lanes(
    events: Sequence[Dict[str, Any]],
    lane_count: int,
    max_event_minutes: int = 0,
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

        # Cap runaway event lengths (e.g. bad upstream end_utc showing an
        # MLB game as 8+ hours) to the user's configured maximum.
        if max_event_minutes > 0:
            max_en = st + dt.timedelta(minutes=max_event_minutes)
            if en > max_en:
                en = max_en

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
            INSERT INTO adb_lanes (provider_code, lane_number, channel_id, event_id, start_utc, stop_utc, playable_id, playable_label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (provider_code, lane_number, channel_id, ev["id"], dt_to_iso(st), dt_to_iso(en), ev.get("playable_id"), ev.get("playable_label")),
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
    team_rules: List[Any] = prefs.get("team_rules") or []
    expand_all_playables: bool = bool(get_setting(conn, "expand_all_playables", False))
    language_preference: str = prefs.get("language_preference", "en")
    amazon_master_enabled: bool = prefs.get("amazon_master_enabled", True)
    max_event_minutes: int = int(get_setting(conn, "max_event_minutes", 0) or 0)
    if expand_all_playables:
        log.info("Expand-all-playables mode ON: one ADB lane per sibling playable instead of one per event.")

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

    # Clear stale lanes for any provider no longer enabled before rebuilding.
    # Previously only enabled providers were cleared (inside the loop), so
    # disabled providers left orphaned rows that kept appearing in exports.
    enabled_codes = {code for code, _ in providers}
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT provider_code FROM adb_lanes")
    existing_codes = {row[0] for row in cur.fetchall()}
    stale_codes = existing_codes - enabled_codes
    for stale in stale_codes:
        log.info("Clearing stale adb_lanes for disabled provider: %s", stale)
        cur.execute("DELETE FROM adb_lanes WHERE provider_code=?", (stale,))
    if stale_codes:
        conn.commit()

    for provider_code, lane_count in providers:
        clear_adb_lanes(conn, provider_code, log)

        if provider_code == "aiv" and not amazon_master_enabled:
            log.info("Skipping provider %s because the Amazon master toggle is disabled", provider_code)
            continue

        # Only enforce enabled_services when the user explicitly set a non-empty allowlist.
        # Check if ANY of the logical services mapped to this ADB provider are enabled.
        #
        # Amazon special-case:
        #   - provider_code 'aiv' should be considered enabled if either:
        #       * 'aiv' is enabled, OR
        #       * any 'aiv_*' logical service is enabled
        if enabled_services:
            if provider_code == "aiv":
                if ("aiv" not in enabled_services) and (not any(s.startswith("aiv_") for s in enabled_services if isinstance(s, str))):
                    log.info("Skipping provider %s because no Amazon services are enabled (need 'aiv' or any 'aiv_*')", provider_code)
                    continue
            else:
                logical_services = get_logical_services_for_adb_provider(provider_code)
                # Check both logical services AND the provider code itself
                if not any(ls in enabled_services for ls in logical_services) and provider_code not in enabled_services:
                    log.info(
                        "Skipping provider %s because none of its logical services %s are in enabled_services",
                        provider_code,
                        logical_services,
                    )
                    continue

        evs = load_events_for_provider(
            conn, provider_code, enabled_services,
            expand_all_playables=expand_all_playables,
            language_preference=language_preference,
        )

        filtered: List[Dict[str, Any]] = []
        null_ts = 0
        for ev in evs:
            st = parse_iso_utc(ev.get("start_utc", "")) or ms_to_dt(ev.get("start_ms"))
            en = parse_iso_utc(ev.get("end_utc", "")) or ms_to_dt(ev.get("end_ms"))
            if not st or not en:
                null_ts += 1
                continue
            if not should_include_event(
                ev.get("classification_json", ""), disabled_sports, disabled_leagues,
                ev.get("raw_attributes_json", ""), team_rules,
            ):
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

        assignments = assign_to_lanes(filtered, lane_count, max_event_minutes)
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
