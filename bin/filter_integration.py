#!/usr/bin/env python3
"""
filter_integration.py - Helper functions for applying user filters to exports

Integrates with user_preferences table to filter events and select best deeplinks.
Now uses logical service mapping to break down "Web" into distinct services.
"""

import json
import re
import sqlite3
from typing import Dict, List, Optional, Any

# Pulls a UUID out of either the current bare-UUID espn_graph_id format or the
# legacy "espn-watch:{playID}:{hash}" format written before 2026-01-23.
_ESPN_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)

try:
    from provider_utils import get_best_deeplink, filter_playables_by_services
except ImportError:
    # Fallback if provider_utils not available
    def filter_playables_by_services(playables, enabled_services=None):
        return playables

    def get_best_deeplink(playables, enabled_services=None):
        return playables[0] if playables else None

try:
    from logical_service_mapper import (
        get_logical_service_for_playable,
        get_service_display_name,
        get_logical_service_priority,
    )
    LOGICAL_SERVICES_AVAILABLE = True
except ImportError:
    print("Warning: logical_service_mapper not available")
    LOGICAL_SERVICES_AVAILABLE = False

    def get_logical_service_for_playable(*args, **kwargs):
        return kwargs.get("provider", "https")

    def get_service_display_name(code):
        return code

    def get_logical_service_priority(code):
        return 25


try:
    from core.service_catalog import DEFAULT_USER_PRIORITY
except ImportError:
    DEFAULT_USER_PRIORITY = {}

try:
    from core.service_catalog import get_canonical_service_code
except ImportError:
    def get_canonical_service_code(service_code):
        return service_code


def get_default_service_priorities() -> Dict[str, int]:
    """
    Get smart default priorities for streaming services (higher = preferred).

    Sourced from core/service_catalog.py — the single source of truth for
    service priorities (see CLAUDE.md) — so exports/lanes and the Filters UI
    agree on which service wins by default.
    """
    return dict(DEFAULT_USER_PRIORITY)


def load_user_preferences(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Load user filter preferences from database.

    Expected keys in user_preferences:
      - enabled_services: JSON list of logical service codes
      - disabled_sports: JSON list
      - disabled_leagues: JSON list
      - team_rules: JSON list of sport/league team selection rules
      - service_priorities: JSON object mapping service code -> int priority
      - amazon_penalty: JSON bool
      - amazon_master_enabled: JSON bool
      - language_preference: JSON string ("en", "es", "both")

    Returns a dict with sane defaults when the table/keys are missing.
    """
    defaults: Dict[str, Any] = {
        "enabled_services": [],
        "disabled_sports": [],
        "disabled_leagues": [],
        "team_rules": [],
        "service_priorities": get_default_service_priorities(),
        "amazon_penalty": True,
        "amazon_master_enabled": True,
        "language_preference": "en",
    }

    try:
        cur = conn.cursor()

        # Check if table exists
        cur.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            return defaults

        raw: Dict[str, Any] = {}
        cur.execute("SELECT key, value FROM user_preferences")
        for key, value in cur.fetchall():
            # Keep raw strings; parse per-key below
            raw[key] = value

        result: Dict[str, Any] = dict(defaults)

        # Lists
        for k in ("enabled_services", "disabled_sports", "disabled_leagues", "team_rules"):
            v = raw.get(k, None)
            if v is None:
                continue
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                result[k] = parsed if isinstance(parsed, list) else defaults[k]
            except Exception:
                result[k] = defaults[k]

        # Upgrade persisted legacy codes on read as well as on save. Several
        # background/export paths consume preferences without a UI save first.
        result["enabled_services"] = [
            get_canonical_service_code(service)
            for service in result["enabled_services"]
        ]

        # Service priorities (merge user overrides onto defaults)
        v = raw.get("service_priorities", None)
        if v is not None:
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                if isinstance(parsed, dict):
                    merged = get_default_service_priorities()
                    merged.update({str(k): int(val) for k, val in parsed.items()})
                    result["service_priorities"] = merged
            except Exception:
                result["service_priorities"] = get_default_service_priorities()

        # Amazon penalty
        v = raw.get("amazon_penalty", None)
        if v is not None:
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                result["amazon_penalty"] = bool(parsed)
            except Exception:
                result["amazon_penalty"] = defaults["amazon_penalty"]

        # Amazon master enabled
        v = raw.get("amazon_master_enabled", None)
        if v is not None:
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                result["amazon_master_enabled"] = bool(parsed)
            except Exception:
                result["amazon_master_enabled"] = defaults["amazon_master_enabled"]

        # Language preference
        v = raw.get("language_preference", None)
        if v is not None:
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                if isinstance(parsed, str) and parsed in ("en", "es", "both"):
                    result["language_preference"] = parsed
            except Exception:
                result["language_preference"] = defaults["language_preference"]

        return result

    except Exception as e:
        print(f"Warning: Could not load user preferences: {e}")
        return defaults

def _norm_filter_value(value: Any) -> str:
    return str(value or "").strip().casefold()


def _event_team_context(event: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str, str]:
    """Return structured competitors plus the event's sport and league.

    Apple competitors are intentionally read from raw_attributes_json instead
    of titles.  Full names such as "Chicago Bulls" are safe identities while
    nickname matching ("Bulls") would collide with college teams.
    """
    raw = event.get("raw_attributes_json") or "{}"
    try:
        attrs = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        attrs = {}
    if not isinstance(attrs, dict):
        attrs = {}

    competitors = attrs.get("competitors") or []
    if not isinstance(competitors, list):
        competitors = []
    competitors = [item for item in competitors if isinstance(item, dict)]

    sport = attrs.get("sport_name") or ""
    league = attrs.get("league_name") or ""

    if not sport:
        try:
            genres = json.loads(event.get("genres_json") or "[]")
            sport = next((item for item in genres if isinstance(item, str) and item), "")
        except Exception:
            pass
    if not league:
        try:
            classifications = json.loads(event.get("classification_json") or "[]")
            league = next(
                (item.get("value") for item in classifications
                 if isinstance(item, dict) and item.get("type") == "league" and item.get("value")),
                "",
            )
        except Exception:
            pass

    return competitors, str(sport or ""), str(league or "")


def _is_team_competitor(competitor: Dict[str, Any]) -> bool:
    return _norm_filter_value(competitor.get("type")) in ("", "team")


def _competitor_matches_selection(
    competitor: Dict[str, Any], selected_team: Dict[str, Any]
) -> bool:
    selected_id = _norm_filter_value(selected_team.get("team_id") or selected_team.get("id"))
    competitor_id = _norm_filter_value(competitor.get("id"))
    if selected_id and competitor_id:
        return selected_id == competitor_id
    return bool(
        _norm_filter_value(selected_team.get("name"))
        and _norm_filter_value(selected_team.get("name"))
        == _norm_filter_value(competitor.get("name"))
    )


def event_passes_team_rules(event: Dict[str, Any], team_rules: List[Any]) -> bool:
    """Apply the rule scoped to this event's sport and league.

    ``include`` keeps matchups containing any selected team. ``exclude``
    removes matchups containing any selected team. Events without structured
    team competitors follow include_unassigned, which defaults to True.
    """
    if not team_rules:
        return True

    competitors, sport, league = _event_team_context(event)
    event_sport = _norm_filter_value(sport)
    event_league = _norm_filter_value(league)

    for rule in team_rules:
        if not isinstance(rule, dict):
            continue
        if _norm_filter_value(rule.get("sport")) != event_sport:
            continue
        if _norm_filter_value(rule.get("league")) != event_league:
            continue
        mode = _norm_filter_value(rule.get("mode"))
        if mode not in ("include", "exclude"):
            continue

        team_competitors = [item for item in competitors if _is_team_competitor(item)]
        if not team_competitors:
            return bool(rule.get("include_unassigned", True))

        selected_teams = rule.get("teams") or []
        selected_teams = [item for item in selected_teams if isinstance(item, dict)]
        has_selected_team = any(
            _competitor_matches_selection(competitor, selected)
            for competitor in team_competitors
            for selected in selected_teams
        )
        return has_selected_team if mode == "include" else not has_selected_team

    return True


def should_include_event(event: Dict[str, Any], preferences: Dict[str, Any]) -> bool:
    """
    Check if event should be included based on user preferences

    Args:
        event: Event dict with genres_json, classification_json, etc.
        preferences: User preferences from load_user_preferences()

    Returns:
        True if event should be included, False if filtered out
    """
    disabled_sports = preferences.get("disabled_sports", [])
    disabled_leagues = preferences.get("disabled_leagues", [])
    team_rules = preferences.get("team_rules", [])

    # Check genres (sports)
    if disabled_sports:
        genres_json = event.get("genres_json", "[]")
        try:
            genres = json.loads(genres_json) if genres_json else []
            for genre in genres:
                if genre in disabled_sports:
                    return False
        except Exception:
            pass

    # Check classifications (leagues)
    if disabled_leagues:
        class_json = event.get("classification_json", "[]")
        try:
            classifications = json.loads(class_json) if class_json else []
            for item in classifications:
                if isinstance(item, dict) and item.get("type") == "league":
                    if item.get("value") in disabled_leagues:
                        return False
        except Exception:
            pass

    if not event_passes_team_rules(event, team_rules):
        return False

    return True


def apply_amazon_penalty(
    playables: List[Dict[str, Any]], 
    amazon_penalty: bool = True
) -> List[Dict[str, Any]]:
    """
    Apply penalty to Amazon Prime Video when direct service alternatives exist.
    
    Amazon often acts as an aggregator, redirecting to services like TNT, TBS, 
    HBO Max, etc. When a direct link to those services is available, prefer it.
    
    Args:
        playables: List of playable dicts (must have 'logical_service' key)
        amazon_penalty: If True, move Amazon to end when alternatives exist
    
    Returns:
        Reordered playables list (or original if penalty disabled)
    """
    if not amazon_penalty or not playables:
        return playables
    
    # as "Amazon" for penalty purposes.
    amazon_services = {"aiv", "aiv_aggregator"}

    # Check if we have non-Amazon options
    has_non_amazon = any(
        p.get("logical_service") not in amazon_services for p in playables
    )
    
    if not has_non_amazon:
        # Only Amazon available, no penalty needed
        return playables
    
    # Separate Amazon from other services
    amazon_playables = [p for p in playables if p.get("logical_service") in amazon_services]
    other_playables = [p for p in playables if p.get("logical_service") not in amazon_services]
    
    # Return non-Amazon first, then Amazon as fallback
    return other_playables + amazon_playables


def _classify_espn_locale(playable: Dict[str, Any]) -> tuple:
    """Return (is_spanish, is_named_deportes) for an ESPN playable.

    Prefers the locale column (populated by migrate_add_locale.py from title
    as well as service_name -- e.g. "En Español" in the title) since ESPN
    Unlimited/MLB.TV/MLB Network playables often share one generic
    service_name across English/Spanish entitlements with nothing else to
    distinguish them. Falls back to the service_name/title text heuristic for
    playables that predate the locale migration.

    locale_fallback overrides is_spanish to False: fix_espn_spanish_only.py
    sets it on Spanish-only ESPN events whose deeplink it has already
    rewritten to the general broadcast (via espn_graph_id/externalId) rather
    than the Spanish-specific playID. The locale column itself is left as
    'es_MX' on purpose (fix_espn_spanish_only.py needs it to re-detect and
    re-fix the row every day after re-scrape resets the deeplink), so without
    this override an "en" language preference would exclude the only,
    already-repaired playable for the event and leave it with zero valid
    links (regression: MLB Unlimited games with only a Spanish broadcast
    showing no valid links under an English filter).

    That override does NOT apply when is_named_deportes is True. "ESPN
    Deportes" is a real, distinctly-branded Spanish-language linear network,
    not an ambiguous generic label with an English broadcast hiding behind
    it -- fix_espn_spanish_only.py's rewrite (point at the externalId/
    espn_graph_id "general broadcast") doesn't change what's actually being
    aired for a genuine Deportes feed, so locale_fallback must not silence
    the language filter for it (regression found spot-checking: a Little
    League game with only an ESPN Deportes broadcast got locale_fallback
    set and started showing under an English-only filter).

    Shared by the language-preference filter and the ESPN channel tiebreak
    sort in get_filtered_playables() so the two can't drift apart (community
    report #787 was caused by exactly that: only one of the two had locale
    detection).
    """
    service_name = (playable.get("service_name") or "").lower()
    title = (playable.get("title") or "").lower()
    locale = (playable.get("locale") or "").lower()
    is_named_deportes = "deportes" in service_name or "español" in service_name
    if locale:
        is_spanish = locale.startswith("es")
    else:
        is_spanish = is_named_deportes or "español" in title
    if playable.get("locale_fallback") and not is_named_deportes:
        is_spanish = False
    return is_spanish, is_named_deportes


def get_filtered_playables(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True,
    language_preference: str = "en",
    amazon_master_enabled: bool = True
) -> List[Dict[str, Any]]:
    """
    Get playables for an event, filtered by enabled services using logical service mapping

    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled logical service codes
        priority_map: Optional dict of service code -> priority (higher = better)
        amazon_penalty: If True, deprioritize Amazon when alternatives exist
        language_preference: Language preference - "en", "es", or "both"
        amazon_master_enabled: If False, ALL Amazon services are disabled regardless of enabled_services

    Returns:
        List of playable dicts, filtered and sorted by priority
    """
    cur = conn.cursor()

    # "Amazon Exclusives" mode: treat AIV playables as a separate logical service
    # but ONLY for events where Amazon Prime Video is the *only* mapped service.
    exclusive_mode = (
        enabled_services
        and ("aiv" not in enabled_services)
    )


    try:
        # Build the SELECT from whichever optional columns this DB actually
        # has migrated, rather than assuming migrate_add_espn_locale_fallback.py
        # and migrate_add_espn_feed_name_column.py either both ran or neither
        # did -- they're independent migrations that can be applied in either
        # order (or not at all, e.g. a test fixture's hand-built schema), so a
        # DB can have one column without the other.
        cur.execute("PRAGMA table_info(playables)")
        available_cols = {row[1] for row in cur.fetchall()}

        base_cols = [
            "playable_id", "provider", "deeplink_play", "deeplink_open",
            "playable_url", "title", "content_id", "priority", "service_name",
            "espn_graph_id", "logical_service", "locale",
        ]
        optional_cols = [c for c in ("locale_fallback", "feed_name") if c in available_cols]
        select_cols = base_cols + optional_cols

        cur.execute(
            f"""
            SELECT {', '.join(select_cols)}
            FROM playables
            WHERE event_id = ?
            ORDER BY priority ASC, playable_id ASC
            """,
            (event_id,),
        )
        rows = cur.fetchall()

        playables: List[Dict[str, Any]] = []
        for row in rows:
            playable: Dict[str, Any] = dict(zip(select_cols, row))
            playable.setdefault("locale_fallback", 0)
            playable.setdefault("feed_name", None)
            playable["event_id"] = event_id

            # Language filtering for ESPN feeds.
            # Prefer the locale column (populated by migrate_add_locale.py from
            # title as well as service_name -- e.g. "En Español" in the title)
            # since ESPN Unlimited playables often share one generic service_name
            # ("ESPN Unlimited") across English/Spanish entitlements with nothing
            # else to distinguish them. Fall back to the service_name/title text
            # heuristic for playables that predate the locale migration.
            if language_preference != "both":
                is_spanish, _ = _classify_espn_locale(playable)

                if language_preference == "en" and is_spanish:
                    continue  # Skip Spanish feeds if user wants English only
                elif language_preference == "es" and not is_spanish:
                    continue  # Skip English feeds if user wants Spanish only

            # Determine logical service for this playable
            # If not already set in database, calculate it
            if not playable.get("logical_service"):
                if LOGICAL_SERVICES_AVAILABLE:
                    logical_service = get_logical_service_for_playable(
                        provider=playable["provider"],
                        deeplink_play=playable["deeplink_play"],
                        deeplink_open=playable["deeplink_open"],
                        playable_url=playable["playable_url"],
                        event_id=event_id,
                        conn=conn,
                        service_name=playable.get("service_name")  # Pass service_name for ESPN differentiation
                    )
                    playable["logical_service"] = logical_service
                else:
                    # Fallback: use raw provider
                    playable["logical_service"] = playable["provider"]

            # AMAZON MASTER TOGGLE: If master toggle is OFF, skip ALL Amazon services
            if not amazon_master_enabled and playable["logical_service"].startswith("aiv"):
                continue

            # Normalize legacy logical_service aliases so they match what the UI/prefs
            # store, e.g. old playables may have 'aiv_fox' while the filter UI saves
            # 'aiv_fox_one'. See core.service_catalog.LEGACY_SERVICE_ALIASES for why
            # this must be the single shared alias table.
            raw_ls = playable["logical_service"]
            canonical_ls = get_canonical_service_code(raw_ls)
            if canonical_ls != raw_ls:
                playable["logical_service"] = canonical_ls  # normalize for downstream use

            # Filter by enabled services
            if enabled_services:  # If list not empty, filter
                ls = playable["logical_service"]
                # Direct match
                if ls in enabled_services:
                    playables.append(playable)
                # OLD-STYLE: 'aiv' alone with no specific aiv_* sub-services = all Amazon allowed.
                # This handles legacy prefs (e.g. ["aiv", "espn_plus"]) that predate the
                # per-service Amazon UI. If the user has explicitly listed any aiv_* sub-services,
                # those are the source of truth and this wildcard does NOT apply.
                elif (
                    ls.startswith("aiv")
                    and "aiv" in enabled_services
                    and not any(s.startswith("aiv_") for s in enabled_services)
                ):
                    playables.append(playable)
                # NOTE: espn_mlb_tv/espn_mlb_network are NOT wildcarded in under
                # 'espn_unlimited' here -- that used to happen (see
                # migrate_backfill_espn_unlimited_granular_tiers.py for why it
                # was removed: it couldn't distinguish "user never saw this
                # checkbox" from "user explicitly unchecked it", so an explicit
                # uncheck of ESPN MLB.TV was silently overridden back on every
                # request -- reported on the forum). Existing users' saved
                # enabled_services were backfilled once by that migration to
                # preserve the old inclusive behavior for anyone who legitimately
                # never considered these tiers; from here on, absence means off,
                # same as every other checkbox.
            else:
                # No filtering - include all
                playables.append(playable)

        # Apply Amazon penalty if enabled
        playables = apply_amazon_penalty(playables, amazon_penalty)

        # ESPN channel prioritization: Prefer main "ESPN" feed over alternates
        # ESPN provides multiple feeds: ESPN (main), ESPN2 (alt commentary), ESPNU, ESPNews, etc.
        # We want to prioritize the main broadcast
        def espn_channel_priority(playable):
            """Return priority score for ESPN channels (lower = better)"""
            service_name = (playable.get("service_name") or "").lower()
            is_spanish, is_named_deportes = _classify_espn_locale(playable)

            # Main ESPN channel gets highest priority
            if service_name == "espn":
                return 0
            # ESPN Deportes -- a real, distinct linear channel (not a generic
            # ambiguous label), so it keeps ranking above alternate English
            # feeds for language_preference="both" users, as before.
            elif is_named_deportes:
                return 1
            # Alternate English feeds
            elif service_name in ("espn2", "espnu", "espnews", "sec network"):
                return 2
            # Unknown/other English feeds (e.g. generic "ESPN Unlimited" labels)
            elif not is_spanish:
                return 3
            # Locale-detected Spanish on a generic, non-Deportes-branded label
            # (e.g. "ESPN Unlimited") ranks last -- only reached in
            # language_preference="both" mode, since "en" mode filters these
            # out entirely above. Keeping this below even unknown English
            # feeds prevents the improved locale detection from letting an
            # ambiguous Spanish entitlement silently outrank a real English
            # alternate (community report #787).
            else:
                return 4

        # Sort by user priorities (if provided) or fallback to system priorities
        if priority_map:
            playables.sort(
                key=lambda p: (
                    -priority_map.get(p["logical_service"], 50),  # User priority (negative for descending)
                    espn_channel_priority(p),  # ESPN channel priority (main > alt)
                    get_logical_service_priority(p["logical_service"])  # System fallback
                )
            )
        elif LOGICAL_SERVICES_AVAILABLE:
            # Fallback to system priorities only + ESPN channel priority
            playables.sort(
                key=lambda p: (
                    espn_channel_priority(p),  # ESPN channel priority (main > alt)
                    get_logical_service_priority(p["logical_service"])
                )
            )

        return playables

    except Exception as e:
        print(f"Warning: Could not load playables for {event_id}: {e}")
        return []


def get_espn_watchgraph_deeplink(
    conn: sqlite3.Connection, event_id: str, apple_deeplink: str
) -> Optional[str]:
    """
    Get ESPN Watch Graph playback ID from playables.espn_graph_id column.
    
    The enrichment process already stored ESPN playback IDs in the playables table.
    This function simply extracts it and builds the correct deeplink.
    
    Args:
        conn: Database connection to fruit_events.db
        event_id: Event ID
        apple_deeplink: Original deeplink (to determine format)
        
    Returns:
        Deeplink with ESPN Watch Graph playback ID, or None if not enriched
    """
    try:
        cur = conn.cursor()
        
        # Get ESPN Graph ID from playables table (already enriched!)
        cur.execute("""
            SELECT espn_graph_id
            FROM playables
            WHERE event_id = ?
              AND provider IN ('sportscenter', 'espn', 'espn+')
              AND espn_graph_id IS NOT NULL
            LIMIT 1
        """, (event_id,))
        
        result = cur.fetchone()
        if not result or not result[0]:
            return None
        
        # Extract playback ID (bare UUID, or legacy espn-watch:{playback_id}:{hash} format)
        espn_graph_id = result[0]
        m = _ESPN_UUID_RE.search(espn_graph_id)
        if not m:
            return None

        playback_id = m.group(0)
        
        # Build deeplink in same format as original
        if apple_deeplink.startswith('sportscenter://'):
            return f"sportscenter://x-callback-url/showWatchStream?playID={playback_id}"
        elif apple_deeplink.startswith('http'):
            return f"https://www.espn.com/watch/player/_/id/{playback_id}"
        else:
            return f"sportscenter://x-callback-url/showWatchStream?playID={playback_id}"
            
    except Exception:
        return None


def get_best_playable_for_event(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True,
    language_preference: str = "en"
) -> Optional[Dict[str, Any]]:
    """
    Get the best playable dict for an event based on user preferences.

    Returns:
        Dict representing the best playable (includes provider, logical_service,
        deeplink_* fields, etc.), or None if nothing suitable.
    """
    playables = get_filtered_playables(
        conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference
    )
    if not playables:
        return None

    # get_filtered_playables already filtered by logical service and sorted by priority
    # Just return the first one (highest priority)
    return playables[0]


def _resolve_deeplink_for_playable(playable: Dict[str, Any]) -> Optional[str]:
    """Resolve a single playable dict (as returned by get_filtered_playables) to its
    playable deeplink, applying the ESPN Watch Graph override (Apple's externalId
    deeplink is frequently broken/wrong; ESPN's own playID is not).

    Shared by get_best_deeplink_for_event() (single "best" pick) and
    get_all_deeplinks_for_event() (expand-all-playables export mode) so the two
    can't drift on how an ESPN deeplink gets corrected.
    """
    deeplink = (
        playable.get("deeplink_play")
        or playable.get("deeplink_open")
        or playable.get("playable_url")
    )

    provider = playable.get("provider") or playable.get("logical_service") or ""
    espn_graph_id = playable.get("espn_graph_id")

    if provider.lower() in ("sportscenter", "espn", "espn+", "espn-plus") and espn_graph_id and deeplink:
        try:
            # Extract playback ID (bare UUID, or legacy espn-watch:{id}:{hash} format)
            m = _ESPN_UUID_RE.search(espn_graph_id)
            if not m:
                raise ValueError("espn_graph_id has no extractable UUID")
            playback_id = m.group(0)

            # Build the correct deeplink format based on original
            if deeplink.startswith('sportscenter://'):
                return f"sportscenter://x-callback-url/showWatchStream?playID={playback_id}"
            elif deeplink.startswith('http'):
                return f"https://www.espn.com/watch/player/_/id/{playback_id}"
            else:
                return f"sportscenter://x-callback-url/showWatchStream?playID={playback_id}"
        except Exception:
            pass  # Fall back to Apple's deeplink if ESPN processing fails

    return deeplink


def get_best_deeplink_for_event(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True,
    language_preference: str = "en"
) -> Optional[str]:
    """
    Get the best deeplink for an event based on user preferences

    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled provider schemes
        priority_map: Optional dict of service code -> priority
        amazon_penalty: If True, deprioritize Amazon when alternatives exist
        language_preference: Language preference ("en", "es", or "both")

    Returns:
        Best deeplink URL, or None if no suitable playables
    """
    best = get_best_playable_for_event(
        conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference
    )
    if not best:
        return None
    return _resolve_deeplink_for_playable(best)


def get_all_deeplinks_for_event(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True,
    language_preference: str = "en",
    amazon_master_enabled: bool = True,
) -> List[Dict[str, Any]]:
    """
    Return every filtered playable for an event, each with its resolved deeplink
    attached under "resolved_deeplink", sorted best-first (same order as
    get_best_playable_for_event's pick).

    Used by the "expand all playables" export mode, which emits one channel per
    playable instead of collapsing to a single best pick like
    get_best_deeplink_for_event() does.

    Returns:
        List of playable dicts (possibly empty), each guaranteed to have a
        non-empty "resolved_deeplink".
    """
    playables = get_filtered_playables(
        conn, event_id, enabled_services, priority_map, amazon_penalty,
        language_preference, amazon_master_enabled,
    )
    out: List[Dict[str, Any]] = []
    for p in playables:
        deeplink = _resolve_deeplink_for_playable(p)
        if not deeplink:
            continue
        p = dict(p)
        p["resolved_deeplink"] = deeplink
        out.append(p)
    return out


def expand_enabled_services_for_amazon(
    conn: sqlite3.Connection, enabled_services: List[str]
) -> List[str]:
    """Resolve Amazon service filtering from enabled_services.

    Handles two historical pref formats:

    1. OLD-STYLE (pre per-service UI): enabled_services contains only 'aiv'
       with no individual aiv_* entries. Treat 'aiv' as a wildcard — all
       Amazon sub-services are allowed. Return enabled_services unchanged;
       get_filtered_playables handles the wildcard check.

    2. NEW-STYLE (per-service UI): enabled_services contains specific aiv_*
       entries alongside 'aiv'. The individual entries are the source of
       truth — 'aiv' is just the master toggle. Return as-is; the explicit
       aiv_* list drives filtering in get_filtered_playables.

    Also normalizes legacy DB aliases (e.g. 'aiv_fox' -> 'aiv_fox_one') via
    core.service_catalog.LEGACY_SERVICE_ALIASES -- the single shared alias
    table also used to normalize playables.logical_service in
    get_filtered_playables() and to normalize saved preferences in
    db/preferences.py. Keep it there, not here: a local copy is how the
    aiv_watch_for_free alias got added to those two but missed here, which
    silently dropped the "Watch for free" Amazon sub-filter for anyone with
    the old code still saved in enabled_services.

    Args:
        conn: Database connection
        enabled_services: List of enabled service codes from user preferences

    Returns:
        Normalized enabled_services list (aliases resolved, no expansion)
    """
    try:
        if not enabled_services:
            return enabled_services

        # Normalize any legacy aliases in the stored list
        normalized = [get_canonical_service_code(s) for s in enabled_services]
        return normalized

    except Exception:
        return enabled_services


# Granular ESPN entitlement tiers that fruit_enrich_espn.py (Step 7c) carves
# out of the generic "espn_unlimited" bucket. Apple's catalog sometimes only
# exposes an event's English broadcast under one of these tiers while the
# event's espn_unlimited-tagged playable is Spanish-only -- so a user who
# only checked "ESPN Unlimited" in Filters (not knowing these exist as
# separate, independently-listed services) gets zero valid links for exactly
# those events even though an English option exists.
ESPN_UNLIMITED_GRANULAR_TIERS = ("espn_mlb_tv", "espn_mlb_network")


def expand_enabled_services_for_espn_unlimited(enabled_services: List[str]) -> List[str]:
    """If 'espn_unlimited' is enabled and none of its granular MLB tiers are
    explicitly listed, treat 'espn_unlimited' as also covering them.

    ONE-TIME USE ONLY, via migrate_backfill_espn_unlimited_granular_tiers.py.
    This used to be applied live on every filter evaluation (mirroring the
    'aiv' -> aiv_* wildcard above), but unlike Amazon's wildcard it couldn't
    actually tell "user never saw this checkbox" apart from "user explicitly
    unchecked it" -- both look identical as an absence from enabled_services.
    That silently overrode an explicit uncheck of ESPN MLB.TV back on every
    request (reported on the forum). The migration calls this once to
    preserve the old inclusive behavior for existing saved preferences, then
    marks itself done; from then on absence means off, like every other
    service.
    """
    if not enabled_services or "espn_unlimited" not in enabled_services:
        return enabled_services
    if any(s in ESPN_UNLIMITED_GRANULAR_TIERS for s in enabled_services):
        return enabled_services
    return enabled_services + [s for s in ESPN_UNLIMITED_GRANULAR_TIERS if s not in enabled_services]


def get_fallback_deeplink(event: Dict[str, Any]) -> Optional[str]:
    """
    Get fallback deeplink from event's raw_attributes_json

    Used when playables table doesn't have data or no match found
    """
    try:
        raw_json = event.get("raw_attributes_json")
        if not raw_json:
            return None

        attrs = json.loads(raw_json)
        playables = attrs.get("playables", [])

        for playable in playables:
            punchout = playable.get("punchoutUrls", {})
            if punchout.get("play"):
                return punchout["play"]
            if punchout.get("open"):
                return punchout["open"]
            if playable.get("playable_url"):
                return playable["playable_url"]

        # Check for Apple TV URL
        apple_url = attrs.get("apple_tv_url")
        if apple_url:
            return apple_url

        return None
    except Exception:
        return None


if __name__ == "__main__":
    # Test the module
    print("Filter Integration Module")
    print("=" * 50)

    # Example preferences
    prefs = {
        "enabled_services": ["sportsonespn", "peacock"],
        "disabled_sports": ["Women's Basketball"],
        "disabled_leagues": ["WNBA"],
    }

    # Example event
    event = {
        "genres_json": json.dumps(["Basketball", "NBA"]),
        "classification_json": json.dumps(
            [
                {"type": "sport", "value": "Basketball"},
                {"type": "league", "value": "NBA"},
            ]
        ),
    }

    print(f"Should include event: {should_include_event(event, prefs)}")

    # Example with filtered sport
    event2 = {
        "genres_json": json.dumps(["Women's Basketball"]),
        "classification_json": json.dumps(
            [
                {"type": "league", "value": "WNBA"},
            ]
        ),
    }

    print(f"Should include women's basketball: {should_include_event(event2, prefs)}")
