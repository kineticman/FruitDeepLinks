#!/usr/bin/env python3
"""
logical_service_mapper.py - Map web playables to logical services

This module provides the core logic for breaking down the "Web" provider
into distinct logical services based on URL host and content metadata.
"""

import sqlite3
import json
import re
from typing import Optional, Dict, Any
from urllib.parse import urlparse

# Logical service definitions
LOGICAL_SERVICE_MAP = {
    # Web-based services by domain
    'peacocktv.com': 'peacock_web',
    'www.peacocktv.com': 'peacock_web',
    'play.hbomax.com': 'max',
    'www.max.com': 'max',
    'f1tv.formula1.com': 'f1tv',
    'tv.apple.com': 'apple_tv',  # Special - needs league lookup
}

# Display names for logical services
SERVICE_DISPLAY_NAMES = {
    # App-based (existing)
    'sportsonespn': 'ESPN+',
    'sportscenter': 'ESPN',
    'peacock': 'Peacock',
    'peacocktv': 'Peacock',
    'pplus': 'Paramount+',
    'aiv': 'Prime Video',
    'gametime': 'Prime Video TNF',
    'cbssportsapp': 'CBS Sports',
    'cbstve': 'CBS',
    'nbcsportstve': 'NBC Sports',
    'foxone': 'FOX Sports',
    'fsapp': 'FOX Sports',
    'dazn': 'DAZN',
    'open.dazn.com': 'DAZN',
    'vixapp': 'ViX',
    'nflctv': 'NFL+',
    'nflmobile': 'NFL',
    'watchtru': 'truTV',
    'watchtnt': 'TNT',
    
    # Web-based (new logical services)
    'peacock_web': 'Peacock (Web)',
    'max': 'Max',
    'f1tv': 'F1 TV',
    'apple_mls': 'Apple MLS',
    'apple_mlb': 'Apple MLB',
    'apple_nba': 'Apple NBA',
    'apple_nhl': 'Apple NHL',
    'apple_other': 'Apple TV+',
    
    # Fallback
    'https': 'Web - Other',
    'http': 'Web - Other',
}


def extract_host_from_url(url: str) -> Optional[str]:
    """Extract hostname from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except:
        return None


def get_league_from_event(conn: sqlite3.Connection, event_id: str) -> Optional[str]:
    """Get league from event's classification_json"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT classification_json 
            FROM events 
            WHERE id = ?
        """, (event_id,))
        
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        
        classifications = json.loads(row[0])
        for item in classifications:
            if isinstance(item, dict) and item.get('type') == 'league':
                league = item.get('value', '').upper()
                # Normalize league names
                if 'MLS' in league:
                    return 'MLS'
                elif 'MLB' in league or 'BASEBALL' in league:
                    return 'MLB'
                elif 'NBA' in league:
                    return 'NBA'
                elif 'NHL' in league or 'HOCKEY' in league:
                    return 'NHL'
        
        return None
    except Exception as e:
        print(f"Error getting league for {event_id}: {e}")
        return None


def get_logical_service_for_playable(
    provider: str,
    deeplink_play: Optional[str],
    deeplink_open: Optional[str],
    playable_url: Optional[str],
    event_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None
) -> str:
    """
    Determine the logical service code for a playable.
    
    Args:
        provider: Raw provider from playables table
        deeplink_play: Play deeplink URL
        deeplink_open: Open deeplink URL
        playable_url: Playable URL
        event_id: Event ID (needed for Apple TV league lookup)
        conn: Database connection (needed for Apple TV league lookup)
    
    Returns:
        Logical service code (e.g., 'apple_mls', 'peacock_web', 'pplus', etc.)
    """
    # Non-web providers: use provider as-is
    if provider not in ('http', 'https', None, ''):
        return provider
    
    # Web providers: analyze URL
    url = deeplink_play or deeplink_open or playable_url
    if not url:
        return 'https'  # Fallback to generic web
    
    host = extract_host_from_url(url)
    if not host:
        return 'https'
    
    # Check if it's a known service
    if host in LOGICAL_SERVICE_MAP:
        service = LOGICAL_SERVICE_MAP[host]
        
        # Special handling for Apple TV - need league
        if service == 'apple_tv':
            if event_id and conn:
                league = get_league_from_event(conn, event_id)
                if league == 'MLS':
                    return 'apple_mls'
                elif league == 'MLB':
                    return 'apple_mlb'
                elif league == 'NBA':
                    return 'apple_nba'
                elif league == 'NHL':
                    return 'apple_nhl'
                else:
                    return 'apple_other'
            else:
                # Can't determine league, fallback
                return 'apple_other'
        
        return service
    
    # Unknown web host
    return 'https'


def get_service_display_name(service_code: str) -> str:
    """Get human-readable display name for a service code"""
    return SERVICE_DISPLAY_NAMES.get(service_code, service_code.upper())


def get_all_logical_services_with_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Query all playables and return counts by logical service.
    
    Returns:
        Dict mapping service_code -> count
    """
    cur = conn.cursor()
    
    # Get all future playables with event info
    cur.execute("""
        SELECT 
            p.provider,
            p.deeplink_play,
            p.deeplink_open,
            p.playable_url,
            p.event_id
        FROM playables p
        JOIN events e ON p.event_id = e.id
        WHERE e.end_utc > datetime('now')
    """)
    
    service_counts = {}
    
    for row in cur.fetchall():
        provider = row[0]
        deeplink_play = row[1]
        deeplink_open = row[2]
        playable_url = row[3]
        event_id = row[4]
        
        service_code = get_logical_service_for_playable(
            provider=provider,
            deeplink_play=deeplink_play,
            deeplink_open=deeplink_open,
            playable_url=playable_url,
            event_id=event_id,
            conn=conn
        )
        
        service_counts[service_code] = service_counts.get(service_code, 0) + 1
    
    return service_counts


def get_logical_service_priority(service_code: str) -> int:
    """
    Get priority for service (lower = higher priority).
    Used for selecting best playable when multiple options available.
    """
    PRIORITY_MAP = {
        # Premium sports services
        'sportsonespn': 0,
        'peacock': 1,
        'peacock_web': 2,  # Web version slightly lower priority
        
        # General streaming
        'pplus': 3,
        'aiv': 4,
        'max': 5,
        
        # Sports-specific
        'cbssportsapp': 6,
        'cbstve': 7,
        'nbcsportstve': 8,
        'foxone': 9,
        'fsapp': 10,
        
        # Apple services
        'apple_mls': 11,
        'apple_mlb': 12,
        'apple_nba': 13,
        'apple_nhl': 14,
        'apple_other': 15,
        
        # Niche/specialty
        'dazn': 16,
        'open.dazn.com': 17,
        'f1tv': 18,
        'vixapp': 19,
        'nflctv': 20,
        'watchtru': 21,
        'watchtnt': 22,
        
        # Generic web (lowest priority)
        'https': 30,
        'http': 31,
    }
    
    return PRIORITY_MAP.get(service_code, 25)


if __name__ == '__main__':
    """Test the logical service mapper"""
    print("="*80)
    print("LOGICAL SERVICE MAPPER - TEST")
    print("="*80)
    print()
    
    DB_PATH = "/app/data/fruit_events.db"
    conn = sqlite3.connect(DB_PATH)
    
    print("Analyzing all playables and mapping to logical services...")
    print()
    
    service_counts = get_all_logical_services_with_counts(conn)
    
    print(f"Found {len(service_counts)} distinct logical services:")
    print("-"*80)
    
    # Sort by count descending
    for service_code, count in sorted(service_counts.items(), key=lambda x: -x[1]):
        display_name = get_service_display_name(service_code)
        priority = get_logical_service_priority(service_code)
        print(f"  {service_code:20s} | {display_name:25s} | {count:4d} playables | priority: {priority:2d}")
    
    print()
    print("="*80)
    
    # Show breakdown of web services specifically
    web_services = {k: v for k, v in service_counts.items() 
                   if k in ('peacock_web', 'max', 'f1tv', 'apple_mls', 'apple_mlb', 
                           'apple_nba', 'apple_nhl', 'apple_other', 'https', 'http')}
    
    if web_services:
        print("WEB SERVICES BREAKDOWN:")
        print("-"*80)
        total_web = sum(web_services.values())
        for service_code, count in sorted(web_services.items(), key=lambda x: -x[1]):
            display_name = get_service_display_name(service_code)
            pct = 100 * count / total_web if total_web > 0 else 0
            print(f"  {display_name:25s} {count:4d} ({pct:5.1f}%)")
        print(f"\n  Total Web Playables: {total_web}")
    
    conn.close()
