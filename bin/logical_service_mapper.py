#!/usr/bin/env python3
"""
logical_service_mapper.py - Map web playables to logical services

This module provides the core logic for breaking down the "Web" provider
into distinct logical services based on URL host and content metadata.

NOW INCLUDES: Amazon channel enrichment via amazon_channels table
"""

import sqlite3
import json
import re
import sys
import os
from typing import Optional, Dict, Any
from urllib.parse import urlparse

# Import canonical service data from the core catalog.
# Falls back to inline dicts if the package isn't on sys.path yet (e.g. CLI use).
try:
    _core = os.path.join(os.path.dirname(__file__), "core")
    if _core not in sys.path:
        sys.path.insert(0, os.path.dirname(__file__))
    from core.service_catalog import (
        DISPLAY_NAMES as SERVICE_DISPLAY_NAMES,
        INTERNAL_PRIORITY as _PRIORITY_MAP,
        get_display_name as get_service_display_name,
        get_internal_priority as get_logical_service_priority,
    )
    _CATALOG_AVAILABLE = True
except ImportError:
    _CATALOG_AVAILABLE = False

# Logical service definitions
LOGICAL_SERVICE_MAP = {
    # Web-based services by domain
    'peacocktv.com': 'peacock_web',
    'www.peacocktv.com': 'peacock_web',
    'play.hbomax.com': 'max',
    'www.max.com': 'max',
    'f1tv.formula1.com': 'f1tv',
    'tv.apple.com': 'apple_tv',  # Special - needs league lookup
    'kayosports.com.au': 'kayo_web',
    'www.kayosports.com.au': 'kayo_web',
    'watch.fanatiz.com': 'fanatiz_web',
    'www.fanatiz.com': 'fanatiz_web',
    'gothamsports.com': 'gotham',
    'www.gothamsports.com': 'gotham',
    'beinsports.com': 'bein',
    'www.beinsports.com': 'bein',
    'watch.nesn.com': 'nesn_web',
    'www.watch.nesn.com': 'nesn_web',
}

# Display names for logical services.
# When _CATALOG_AVAILABLE is True these are already imported from service_catalog;
# this inline dict is only used as a last-resort fallback.
if not _CATALOG_AVAILABLE:
    SERVICE_DISPLAY_NAMES = {
        'sportsonespn': 'ESPN+', 'sportscenter': 'ESPN+', 'espn_linear': 'ESPN (Linear)',
        'espn_plus': 'ESPN+', 'peacock': 'Peacock', 'peacocktv': 'Peacock',
        'peacock_web': 'Peacock (Web)', 'pplus': 'Paramount+', 'aiv': 'Prime Video',
        'gametime': 'NBA', 'cbssportsapp': 'CBS Sports', 'cbstve': 'CBS',
        'nbcsportstve': 'NBC Sports', 'foxone': 'FOX Sports (App)', 'fsapp': 'FOX Sports (Alt)',
        'dazn': 'DAZN', 'open.dazn.com': 'DAZN', 'vixapp': 'ViX', 'nflctv': 'NFL+',
        'nflmobile': 'NFL', 'watchtru': 'truTV', 'watchtnt': 'TNT', 'watchtbs': 'TBS',
        'ncaa_march_madness': 'NCAA March Madness', 'marquee': 'Marquee Sports Network',
        'nba': 'NBA League Pass', 'mlb': 'MLB.TV', 'nhl': 'NHL.TV',
        'victory': 'Victory+', 'gotham': 'Gotham Sports', 'bein': 'beIN Sports',
        'max': 'Max', 'f1tv': 'F1 TV', 'kayo_web': 'Kayo Sports', 'fanatiz_web': 'Fanatiz Soccer',
        'apple_mls': 'Apple MLS', 'apple_mlb': 'Apple MLB', 'apple_nba': 'Apple NBA',
        'apple_nhl': 'Apple NHL', 'apple_f1': 'Formula 1 (Apple TV)', 'apple_other': 'Apple TV+',
        'nesn': 'NESN 360', 'nesn_web': 'NESN 360',
        'aiv_prime': 'Amazon - Prime Exclusive', 'aiv_nba_league_pass': 'Amazon - NBA League Pass',
        'aiv_peacock': 'Amazon - Peacock', 'aiv_dazn': 'Amazon - DAZN',
        'aiv_fox': 'Amazon - FOX One', 'aiv_fox_one': 'Amazon - FOX One',
        'aiv_vix_premium': 'Amazon - ViX Premium', 'aiv_vix': 'Amazon - ViX',
        'aiv_tennis_channel': 'Amazon - Tennis Channel', 'aiv_fanduel': 'Amazon - FanDuel Sports Network',
        'aiv_max': 'Amazon - Max', 'aiv_paramount_plus': 'Amazon - Paramount+',
        'aiv_willow': 'Amazon - Willow TV', 'aiv_wnba_league_pass': 'Amazon - WNBA League Pass',
        'aiv_squash': 'Amazon - SquashTV', 'aiv_free': 'Amazon - Free with Ads',
        'aiv_aggregator': 'Amazon - Unknown', 'https': 'Web - Other', 'http': 'Web - Other',
    }


def extract_host_from_url(url: str) -> Optional[str]:
    """Extract hostname from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except:
        return None


def extract_gti_from_deeplink(deeplink: str) -> Optional[str]:
    """Extract GTI from Amazon deeplink (broadcast or main)
    
    Amazon deeplinks contain GTIs in two places:
    - broadcast= parameter (preferred - specific to live event)
    - gti= parameter (fallback - series/show page)
    """
    if not deeplink:
        return None
    
    try:
        # Try broadcast GTI first (for live events)
        broadcast_match = re.search(r'broadcast=(amzn1\.dv\.gti\.[0-9a-f-]{36})', deeplink)
        if broadcast_match:
            return broadcast_match.group(1)
        
        # Fall back to main GTI
        main_match = re.search(r'[?&]gti=(amzn1\.dv\.gti\.[0-9a-f-]{36})', deeplink)
        if main_match:
            return main_match.group(1)
    except Exception:
        pass
    
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
        cur.close()  # CRITICAL FIX: Close cursor after fetching
        
        if not row or not row[0]:
            return None
        
        classifications = json.loads(row[0])
        for item in classifications:
            if isinstance(item, dict) and item.get('type') == 'sport':
                sport = item.get('value', '').upper()
                if 'MOTORSPORT' in sport:
                    return 'MOTORSPORTS'
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


def get_amazon_service_for_playable(
    conn: sqlite3.Connection,
    deeplink_play: Optional[str],
    deeplink_open: Optional[str]
) -> Optional[str]:
    """Get Amazon service from amazon_channels table
    
    Args:
        conn: Database connection
        deeplink_play: Play deeplink URL
        deeplink_open: Open deeplink URL
    
    Returns:
        Logical service code (e.g., 'aiv_nba_league_pass') or None if not found
    """
    # Extract GTI from deeplink
    gti = extract_gti_from_deeplink(deeplink_play or deeplink_open or '')
    
    if not gti:
        return None
    
    try:
        # Check if amazon_channels table exists
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='amazon_channels'
        """)
        if not cur.fetchone():
            cur.close()
            return None
        
        # Query for service mapping
        cur.execute("""
            SELECT s.logical_service
            FROM amazon_channels ac
            JOIN amazon_services s ON ac.channel_id = s.amazon_channel_id
            WHERE ac.gti = ? AND ac.is_stale = 0
        """, (gti,))
        
        row = cur.fetchone()
        cur.close()
        
        if row and row[0]:
            return row[0]
    except Exception as e:
        # Silently fail if amazon_channels not available
        # This allows graceful degradation for deployments without scraper
        pass
    
    return None


def get_logical_service_for_playable(
    provider: str,
    deeplink_play: Optional[str],
    deeplink_open: Optional[str],
    playable_url: Optional[str],
    event_id: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
    service_name: Optional[str] = None
) -> str:
    """
    Determine the logical service code for a playable.
    
    Args:
        provider: Raw provider from playables table
        deeplink_play: Play deeplink URL
        deeplink_open: Open deeplink URL
        playable_url: Playable URL
        event_id: Event ID (needed for Apple TV league lookup)
        conn: Database connection (needed for Apple TV league lookup and Amazon enrichment)
        service_name: Service name from playables table (used for ESPN differentiation)
    
    Returns:
        Logical service code (e.g., 'espn_linear', 'espn_plus', 'aiv_nba_league_pass', etc.)
    """
    # ESPN: differentiate linear TV channels from streaming services
    if provider == 'sportscenter':
        if service_name:
            # Streaming services: ESPN+, ESPN Unlimited, and digital overflow content
            # ACC Extra, SEC Plus are digital-only content requiring ESPN+ or ESPN Unlimited
            if any(x in service_name for x in ['ESPN+', 'Unlimited', 'Extra', 'Plus V2']):
                return 'espn_plus'
            # Linear channels: ESPN, ESPN2, ESPN Deportes, ESPNU, ESPNews, ACC Network, SEC Network
            else:
                return 'espn_linear'
        # Fallback if no service_name
        return 'sportscenter'
    
    # Amazon: enrich with channel data (NEW)
    if provider == 'aiv':
        if conn:
            # Try to get specific Amazon service from scraper data
            amazon_service = get_amazon_service_for_playable(conn, deeplink_play, deeplink_open)
            if amazon_service:
                return amazon_service
        
        # Fallback: unknown Amazon = aggregator
        # This maintains current behavior for unmapped/404 content
        return 'aiv_aggregator'
    
    # Kayo provider: map to kayo_web
    if provider == 'kayo':
        return 'kayo_web'
    
    # Victory+ provider: map to victory
    if provider == 'victory':
        return 'victory'
    
    # Gotham Sports provider: map to gotham
    if provider == 'gotham':
        return 'gotham'
    
    # beIN Sports provider: map to bein
    if provider == 'bein':
        return 'bein'
    
    # NESN provider: single service with multiple playables
    if provider == 'nesn':
        return 'nesn'
    
    # MLB At Bat provider (Apple TV uses mlbatbat:// scheme for MLB.TV)
    if provider == 'mlbatbat':
        return 'mlb'

    # NCAA March Madness Live provider (Apple TV uses ncaamml:// scheme)
    if provider == 'ncaamml':
        return 'ncaa_march_madness'
    
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
                elif league == 'MOTORSPORTS':
                    return 'apple_f1'
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
            p.event_id,
            p.service_name,
            p.logical_service
        FROM playables p
        JOIN events e ON p.event_id = e.id
        WHERE e.end_utc > datetime('now')
    """)
    
    # CRITICAL FIX: Fetch ALL rows first before processing
    # This prevents SQLite lock when get_logical_service_for_playable 
    # calls get_league_from_event or get_amazon_service_for_playable which create another cursor
    all_rows = cur.fetchall()
    
    service_counts = {}
    event_services = {}  # Track which services each event has
    
    for row in all_rows:  # Changed from cur.fetchall() to all_rows
        provider = row[0]
        deeplink_play = row[1]
        deeplink_open = row[2]
        playable_url = row[3]
        event_id = row[4]
        service_name = row[5]
        logical_service = row[6]  # Use pre-calculated logical_service if available
        
        # Use stored logical_service if available, otherwise calculate it
        if logical_service:
            service_code = logical_service
        else:
            service_code = get_logical_service_for_playable(
                provider=provider,
                deeplink_play=deeplink_play,
                deeplink_open=deeplink_open,
                playable_url=playable_url,
                event_id=event_id,
                conn=conn,
                service_name=service_name
            )
        
        service_counts[service_code] = service_counts.get(service_code, 0) + 1
        
        # Track which services each event has (for Amazon Exclusives detection)
        if event_id not in event_services:
            event_services[event_id] = set()
        event_services[event_id].add(service_code)
    
    return service_counts


# Fallback implementations used only when core.service_catalog is unavailable.
if not _CATALOG_AVAILABLE:
    def get_service_display_name(service_code: str) -> str:
        return SERVICE_DISPLAY_NAMES.get(service_code, service_code.upper())

    def get_logical_service_priority(service_code: str) -> int:
        _FALLBACK = {
            'espn_linear': 0, 'sportsonespn': 1, 'espn_plus': 1, 'sportscenter': 1,
            'peacock': 2, 'peacock_web': 3, 'pplus': 4, 'max': 5,
            'aiv_free': 1, 'aiv_prime': 4, 'aiv_peacock': 5, 'aiv_max': 5,
            'cbssportsapp': 6, 'cbstve': 7, 'nbcsportstve': 8,
            'foxone': 9, 'aiv_fox': 9, 'aiv_fox_one': 9, 'fsapp': 10,
            'apple_mls': 11, 'apple_mlb': 12, 'apple_nba': 13, 'apple_nhl': 14,
            'apple_f1': 15, 'apple_other': 16, 'dazn': 16, 'aiv_dazn': 16,
            'open.dazn.com': 17, 'f1tv': 18, 'kayo_web': 19, 'bein': 19,
            'victory': 19, 'nesn': 19, 'nesn_web': 19, 'fanatiz_web': 20,
            'gotham': 20, 'marquee': 20, 'vixapp': 21,
            'aiv_vix_premium': 21, 'aiv_vix': 21,
            'aiv_tennis_channel': 22, 'aiv_fanduel': 22, 'nflctv': 22,
            'watchtru': 23, 'ncaa_march_madness': 24, 'watchtnt': 24, 'watchtbs': 25,
            'nba': 26, 'aiv_nba_league_pass': 26, 'gametime': 26, 'mlb': 26, 'nhl': 26,
            'aiv': 27, 'aiv_aggregator': 27, 'https': 30, 'http': 31,
        }
        return _FALLBACK.get(service_code, 25)


if __name__ == '__main__':
    """Test the logical service mapper"""
    print("="*80)
    print("LOGICAL SERVICE MAPPER - TEST (with Amazon enrichment)")
    print("="*80)
    print()
    
    DB_PATH = "/app/data/fruit_events.db"
    conn = sqlite3.connect(DB_PATH)
    
    # Check if Amazon tables exist
    cur = conn.cursor()
    cur.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='amazon_channels'
    """)
    has_amazon = cur.fetchone() is not None
    cur.close()
    
    if has_amazon:
        print("✓ Amazon channel data available")
    else:
        print("⚠ Amazon channel data NOT available (will use aggregator fallback)")
    print()
    
    print("Analyzing all playables and mapping to logical services...")
    print()
    
    service_counts = get_all_logical_services_with_counts(conn)
    
    print(f"Found {len(service_counts)} distinct logical services:")
    print("-"*80)
    
    # Sort by count descending
    for service_code, count in sorted(service_counts.items(), key=lambda x: -x[1]):
        display_name = get_service_display_name(service_code)
        priority = get_logical_service_priority(service_code)
        print(f"  {service_code:25s} | {display_name:30s} | {count:4d} playables | priority: {priority:2d}")
    
    print()
    print("="*80)
    
    # Show breakdown of Amazon services specifically
    amazon_services = {k: v for k, v in service_counts.items() 
                      if k.startswith('aiv')}
    
    if amazon_services:
        print("AMAZON SERVICES BREAKDOWN:")
        print("-"*80)
        total_amazon = sum(amazon_services.values())
        for service_code, count in sorted(amazon_services.items(), key=lambda x: -x[1]):
            display_name = get_service_display_name(service_code)
            priority = get_logical_service_priority(service_code)
            pct = 100 * count / total_amazon if total_amazon > 0 else 0
            print(f"  {display_name:30s} {count:4d} ({pct:5.1f}%) | priority: {priority:2d}")
        print(f"\n  Total Amazon Playables: {total_amazon}")
        
        # Calculate how many are properly mapped vs aggregator
        mapped = sum(v for k, v in amazon_services.items() if k not in ('aiv', 'aiv_aggregator'))
        aggregator = sum(v for k, v in amazon_services.items() if k in ('aiv', 'aiv_aggregator'))
        if total_amazon > 0:
            print(f"  Mapped to specific services: {mapped} ({100*mapped/total_amazon:.1f}%)")
            print(f"  Aggregator/unknown: {aggregator} ({100*aggregator/total_amazon:.1f}%)")
    
    print()
    print("="*80)
    
    # Show breakdown of web services specifically
    web_services = {k: v for k, v in service_counts.items()
                   if k in ('peacock_web', 'max', 'f1tv', 'apple_mls', 'apple_mlb',
                           'apple_nba', 'apple_nhl', 'apple_f1', 'apple_other', 'https', 'http')}
    
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
