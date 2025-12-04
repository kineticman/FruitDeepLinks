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
        get_logical_service_priority
    )
    LOGICAL_SERVICES_AVAILABLE = True
except ImportError:
    print("Warning: logical_service_mapper not available")
    LOGICAL_SERVICES_AVAILABLE = False
    def get_logical_service_for_playable(*args, **kwargs):
        return kwargs.get('provider', 'https')
    def get_service_display_name(code):
        return code
    def get_logical_service_priority(code):
        return 25


def load_user_preferences(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Load user filter preferences from database
    
    Returns:
        {
            "enabled_services": ["sportsonespn", "peacock", ...],
            "disabled_sports": ["Women's Basketball", ...],
            "disabled_leagues": ["WNBA", ...]
        }
    """
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_preferences'")
        if not cur.fetchone():
            return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}
        
        prefs = {}
        cur.execute("SELECT key, value FROM user_preferences")
        for row in cur.fetchall():
            key = row[0]
            value = row[1]
            try:
                prefs[key] = json.loads(value) if value else []
            except:
                prefs[key] = []
        
        return {
            "enabled_services": prefs.get("enabled_services", []),
            "disabled_sports": prefs.get("disabled_sports", []),
            "disabled_leagues": prefs.get("disabled_leagues", [])
        }
    except Exception as e:
        print(f"Warning: Could not load user preferences: {e}")
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}


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
        except:
            pass
    
    # Check classifications (leagues)
    if disabled_leagues:
        class_json = event.get("classification_json", "[]")
        try:
            classifications = json.loads(class_json) if class_json else []
            for item in classifications:
                if isinstance(item, dict) and item.get('type') == 'league':
                    if item.get('value') in disabled_leagues:
                        return False
        except:
            pass
    
    return True


def get_filtered_playables(conn: sqlite3.Connection, event_id: str, 
                           enabled_services: List[str]) -> List[Dict[str, Any]]:
    """
    Get playables for an event, filtered by enabled services using logical service mapping
    
    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled logical service codes
    
    Returns:
        List of playable dicts, filtered and sorted by priority
    """
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT playable_id, provider, deeplink_play, deeplink_open, 
                   playable_url, title, content_id, priority
            FROM playables
            WHERE event_id = ?
            ORDER BY priority ASC, playable_id ASC
        """, (event_id,))
        
        playables = []
        for row in cur.fetchall():
            playable = {
                "playable_id": row[0],
                "provider": row[1],
                "deeplink_play": row[2],
                "deeplink_open": row[3],
                "playable_url": row[4],
                "title": row[5],
                "content_id": row[6],
                "priority": row[7],
                "event_id": event_id
            }
            
            # Determine logical service for this playable
            if LOGICAL_SERVICES_AVAILABLE:
                logical_service = get_logical_service_for_playable(
                    provider=playable["provider"],
                    deeplink_play=playable["deeplink_play"],
                    deeplink_open=playable["deeplink_open"],
                    playable_url=playable["playable_url"],
                    event_id=event_id,
                    conn=conn
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
        
        # Sort by logical service priority
        if LOGICAL_SERVICES_AVAILABLE:
            playables.sort(key=lambda p: get_logical_service_priority(p["logical_service"]))
        
        return playables
        
    except Exception as e:
        print(f"Warning: Could not load playables for {event_id}: {e}")
        return []


def get_best_deeplink_for_event(conn: sqlite3.Connection, event_id: str,
                                 enabled_services: List[str]) -> Optional[str]:
    """
    Get the best deeplink for an event based on user preferences
    
    Args:
        conn: Database connection
        event_id: Event ID
        enabled_services: List of enabled provider schemes
    
    Returns:
        Best deeplink URL, or None if no suitable playables
    """
    playables = get_filtered_playables(conn, event_id, enabled_services)
    
    if not playables:
        return None
    
    # Get best playable
    best = get_best_deeplink(playables, enabled_services)
    
    if not best:
        return None
    
    # Return best deeplink
    return best.get("deeplink_play") or best.get("deeplink_open") or best.get("playable_url")


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
    except:
        return None


if __name__ == '__main__':
    # Test the module
    print("Filter Integration Module")
    print("="*50)
    
    import sqlite3
    
    # Example preferences
    prefs = {
        "enabled_services": ["sportsonespn", "peacock"],
        "disabled_sports": ["Women's Basketball"],
        "disabled_leagues": ["WNBA"]
    }
    
    # Example event
    event = {
        "genres_json": json.dumps(["Basketball", "NBA"]),
        "classification_json": json.dumps([
            {"type": "sport", "value": "Basketball"},
            {"type": "league", "value": "NBA"}
        ])
    }
    
    print(f"Should include event: {should_include_event(event, prefs)}")
    
    # Example with filtered sport
    event2 = {
        "genres_json": json.dumps(["Women's Basketball"]),
        "classification_json": json.dumps([
            {"type": "league", "value": "WNBA"}
        ])
    }
    
    print(f"Should include women's basketball: {should_include_event(event2, prefs)}")
