#!/usr/bin/env python3
"""
filter_integration.py - Helper functions for applying user filters to exports

Integrates with user_preferences table to filter events and select best deeplinks.
Now uses logical service mapping to break down "Web" into distinct services.
"""

import json
import sqlite3
from typing import Dict, List, Optional, Any

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


def get_default_service_priorities() -> Dict[str, int]:
    """
    Get smart default priorities for streaming services.
    
    Priority Tiers (1-100 scale):
    - 90-100: Premium direct services (ESPN+, Peacock, Paramount+, etc.)
    - 70-89: Cable/network services (TNT, TBS, NBC, Fox, etc.)
    - 50-69: League-specific services (NBA League Pass, MLB.TV, etc.)
    - 30-49: Free/broadcast services (ABC, NBC broadcast, etc.)
    - 10-29: Aggregators with redirects (Amazon Prime Video)
    - 1-9: Fallback/generic web services
    
    Returns:
        Dict mapping service codes to priority values (higher = preferred)
    """
    return {
        # Tier 1: Premium Sports Services (90-100)
        "sportsonespn": 100,      # ESPN+ - comprehensive sports
        "peacock": 98,             # Peacock - NBC Sports, Premier League
        "peacock_web": 98,         # Peacock web version
        "pplus": 96,               # Paramount+ - CBS Sports, Champions League
        "paramount_web": 96,       # Paramount+ web version
        "cbs_web": 95,             # CBS Sports
        "max": 94,                 # Max (HBO Max) - Sports coverage
        "apple_mls": 92,           # Apple MLS Season Pass
        "apple_mlb": 92,           # Apple MLB Friday Night Baseball
        
        # Tier 2: Cable/Network Sports (70-89)
        "watchtnt": 88,            # TNT - NBA, NHL, MLB
        "watchtru": 87,            # TruTV - March Madness
        "watchtbs": 86,            # TBS - MLB, NBA
        "fox_web": 85,             # Fox Sports
        "fs1": 84,                 # Fox Sports 1
        "fs2": 83,                 # Fox Sports 2
        "nbcsports": 82,           # NBC Sports
        "usanetwork": 81,          # USA Network
        "golf": 80,                # Golf Channel
        "espn": 79,                # ESPN cable
        "espn2": 78,               # ESPN2
        "espnu": 77,               # ESPNU
        "btn": 76,                 # Big Ten Network
        "accnetwork": 75,          # ACC Network
        "secnetwork": 74,          # SEC Network
        
        # Tier 3: League-Specific Services (50-69)
        "nba": 68,                 # NBA League Pass
        "nhl": 67,                 # NHL.TV
        "mlb": 66,                 # MLB.TV
        "f1tv": 65,                # F1 TV Pro
        "dazn": 64,                # DAZN
        "kayo_web": 63,            # Kayo Sports (Australia)
        "bein": 63,                # beIN Sports (international/regional)
        "fanatiz_web": 62,         # Fanatiz Soccer (Latin America)
        "victory": 62,             # Victory+ (WHL, LOVB)
        "gotham": 62,              # Gotham Sports (MSG/YES Network)
        "fubo": 61,                # FuboTV
        "sling": 60,               # Sling TV
        
        # Tier 4: Free/Broadcast (30-49)
        "abc": 48,                 # ABC (free broadcast)
        "nbc": 47,                 # NBC (free broadcast)
        "cbs": 46,                 # CBS (free broadcast)
        "fox": 45,                 # Fox (free broadcast)
        
        # Tier 5: Aggregators (10-29)
        "aiv": 15,                 # Amazon Prime Video - often redirects to other services
        
        # Tier 6: Generic/Fallback (1-9)
        "https": 5,                # Generic web link
        "http": 4,                 # Generic web link
        "web": 3,                  # Generic web service
    }


def load_user_preferences(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Load user filter preferences from database.

    Expected keys in user_preferences:
      - enabled_services: JSON list of logical service codes
      - disabled_sports: JSON list
      - disabled_leagues: JSON list
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
        for k in ("enabled_services", "disabled_sports", "disabled_leagues"):
            v = raw.get(k, None)
            if v is None:
                continue
            try:
                parsed = json.loads(v) if isinstance(v, str) else v
                result[k] = parsed if isinstance(parsed, list) else defaults[k]
            except Exception:
                result[k] = defaults[k]

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
        cur.execute(
            """
            SELECT playable_id, provider, deeplink_play, deeplink_open,
                   playable_url, title, content_id, priority, service_name, espn_graph_id,
                   logical_service
            FROM playables
            WHERE event_id = ?
            ORDER BY priority ASC, playable_id ASC
            """,
            (event_id,),
        )

        playables: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            playable: Dict[str, Any] = {
                "playable_id": row[0],
                "provider": row[1],
                "deeplink_play": row[2],
                "deeplink_open": row[3],
                "playable_url": row[4],
                "title": row[5],
                "content_id": row[6],
                "priority": row[7],
                "service_name": row[8],
                "espn_graph_id": row[9],
                "logical_service": row[10],  # Read from database
                "event_id": event_id,
            }

            # Language filtering for ESPN feeds
            # ESPN uses service_name like "ESPN", "ESPN2", "ESPN Deportes"
            # Filter based on language preference
            if language_preference != "both":
                service_name = (playable.get("service_name") or "").lower()
                is_spanish = "deportes" in service_name or "español" in service_name
                
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

            # Filter by enabled services
            if enabled_services:  # If list not empty, filter
                if playable["logical_service"] in enabled_services:
                    playables.append(playable)
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
            
            # Main ESPN channel gets highest priority
            if service_name == "espn":
                return 0
            # ESPN Deportes (Spanish) - second priority for Spanish speakers
            elif "deportes" in service_name or "español" in service_name:
                return 1
            # Alternate English feeds
            elif service_name in ("espn2", "espnu", "espnews", "sec network"):
                return 2
            # Unknown/other
            else:
                return 3

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
        
        # Extract playback ID from format: espn-watch:{playback_id}
        espn_graph_id = result[0]
        if not espn_graph_id.startswith('espn-watch:'):
            return None
        
        playback_id = espn_graph_id.replace('espn-watch:', '', 1)
        
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

    # Get base deeplink
    deeplink = (
        best.get("deeplink_play")
        or best.get("deeplink_open")
        or best.get("playable_url")
    )
    
    # ESPN Watch Graph override: Use ESPN's playback ID instead of Apple's externalId
    # Check if this is an ESPN/sportscenter event and has ESPN Graph ID
    provider = best.get("provider") or best.get("logical_service") or ""
    espn_graph_id = best.get("espn_graph_id")
    
    if provider.lower() in ("sportscenter", "espn", "espn+", "espn-plus") and espn_graph_id and deeplink:
        try:
            # Extract playback ID from espn-watch:PLAYBACK_ID format
            playback_id = espn_graph_id.replace("espn-watch:", "", 1)
            
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

