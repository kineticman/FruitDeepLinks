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
        "fubo": 63,                # FuboTV
        "sling": 62,               # Sling TV
        
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
    Load user filter preferences from database

    Returns:
        {
            "enabled_services": ["sportsonespn", "peacock", ...],
            "disabled_sports": ["Women's Basketball", ...],
            "disabled_leagues": ["WNBA", ...],
            "service_priorities": {"sportsonespn": 100, "peacock": 98, ...},
            "amazon_penalty": True
        }
    """
    try:
        cur = conn.cursor()

        # Check if table exists
        cur.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='user_preferences'"
        )
        if not cur.fetchone():
            return {
                "enabled_services": [],
                "disabled_sports": [],
                "disabled_leagues": [],
                "service_priorities": get_default_service_priorities(),
                "amazon_penalty": True,
            }

        prefs: Dict[str, Any] = {}
        cur.execute("SELECT key, value FROM user_preferences")
        for row in cur.fetchall():
            key = row[0]
            value = row[1]
            try:
                prefs[key] = json.loads(value) if value else []
            except Exception:
                prefs[key] = []

        # Parse special keys
        result = {
            "enabled_services": prefs.get("enabled_services", []),
            "disabled_sports": prefs.get("disabled_sports", []),
            "disabled_leagues": prefs.get("disabled_leagues", []),
        }
        
        # Service priorities (with smart defaults)
        if "service_priorities" in prefs:
            try:
                custom_priorities = json.loads(prefs["service_priorities"]) if isinstance(prefs["service_priorities"], str) else prefs["service_priorities"]
                # Merge with defaults (user values override defaults)
                default_priorities = get_default_service_priorities()
                default_priorities.update(custom_priorities)
                result["service_priorities"] = default_priorities
            except Exception:
                result["service_priorities"] = get_default_service_priorities()
        else:
            result["service_priorities"] = get_default_service_priorities()
        
        # Amazon penalty flag
        if "amazon_penalty" in prefs:
            try:
                result["amazon_penalty"] = bool(json.loads(prefs["amazon_penalty"]) if isinstance(prefs["amazon_penalty"], str) else prefs["amazon_penalty"])
            except Exception:
                result["amazon_penalty"] = True
        else:
            result["amazon_penalty"] = True
        
        return result
        
    except Exception as e:
        print(f"Warning: Could not load user preferences: {e}")
        return {
            "enabled_services": [],
            "disabled_sports": [],
            "disabled_leagues": [],
            "service_priorities": get_default_service_priorities(),
            "amazon_penalty": True,
        }


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
    
    # Check if we have non-Amazon options
    has_non_amazon = any(
        p.get("logical_service") != "aiv" for p in playables
    )
    
    if not has_non_amazon:
        # Only Amazon available, no penalty needed
        return playables
    
    # Separate Amazon from other services
    amazon_playables = [p for p in playables if p.get("logical_service") == "aiv"]
    other_playables = [p for p in playables if p.get("logical_service") != "aiv"]
    
    # Return non-Amazon first, then Amazon as fallback
    return other_playables + amazon_playables


def get_filtered_playables(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True
) -> List[Dict[str, Any]]:
    """
    Get playables for an event, filtered by enabled services using logical service mapping

    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled logical service codes
        priority_map: Optional dict of service code -> priority (higher = better)
        amazon_penalty: If True, deprioritize Amazon when alternatives exist

    Returns:
        List of playable dicts, filtered and sorted by priority
    """
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT playable_id, provider, deeplink_play, deeplink_open,
                   playable_url, title, content_id, priority
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
                "event_id": event_id,
            }

            # Determine logical service for this playable
            if LOGICAL_SERVICES_AVAILABLE:
                logical_service = get_logical_service_for_playable(
                    provider=playable["provider"],
                    deeplink_play=playable["deeplink_play"],
                    deeplink_open=playable["deeplink_open"],
                    playable_url=playable["playable_url"],
                    event_id=event_id,
                    conn=conn,
                )
                playable["logical_service"] = logical_service
            else:
                # Fallback: use raw provider
                playable["logical_service"] = playable["provider"]

            # Filter by enabled services
            if enabled_services:  # If list not empty, filter
                if playable["logical_service"] in enabled_services:
                    playables.append(playable)
            else:
                # No filtering - include all
                playables.append(playable)

        # Apply Amazon penalty if enabled
        playables = apply_amazon_penalty(playables, amazon_penalty)

        # Sort by user priorities (if provided) or fallback to system priorities
        if priority_map:
            playables.sort(
                key=lambda p: (
                    -priority_map.get(p["logical_service"], 50),  # User priority (negative for descending)
                    get_logical_service_priority(p["logical_service"])  # System fallback
                )
            )
        elif LOGICAL_SERVICES_AVAILABLE:
            # Fallback to system priorities only
            playables.sort(
                key=lambda p: get_logical_service_priority(p["logical_service"])
            )

        return playables

    except Exception as e:
        print(f"Warning: Could not load playables for {event_id}: {e}")
        return []


def get_best_playable_for_event(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Get the best playable dict for an event based on user preferences.

    Returns:
        Dict representing the best playable (includes provider, logical_service,
        deeplink_* fields, etc.), or None if nothing suitable.
    """
    playables = get_filtered_playables(
        conn, event_id, enabled_services, priority_map, amazon_penalty
    )
    if not playables:
        return None

    # get_filtered_playables already filtered by logical service and sorted by priority
    # Just return the first one (highest priority)
    return playables[0]


def get_best_deeplink_for_event(
    conn: sqlite3.Connection, event_id: str, enabled_services: List[str],
    priority_map: Optional[Dict[str, int]] = None,
    amazon_penalty: bool = True
) -> Optional[str]:
    """
    Get the best deeplink for an event based on user preferences

    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled provider schemes
        priority_map: Optional dict of service code -> priority
        amazon_penalty: If True, deprioritize Amazon when alternatives exist

    Returns:
        Best deeplink URL, or None if no suitable playables
    """
    best = get_best_playable_for_event(
        conn, event_id, enabled_services, priority_map, amazon_penalty
    )
    if not best:
        return None

    # Return best deeplink
    return (
        best.get("deeplink_play")
        or best.get("deeplink_open")
        or best.get("playable_url")
    )


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

