#!/usr/bin/env python3
"""
provider_utils.py - Utilities for handling streaming provider deeplinks

Extracts provider from URL schemes and provides human-readable names.
Display names and priorities delegate to core.service_catalog.
"""

import sys
import os

# Prefer the canonical catalog; fall back gracefully if unavailable.
try:
    _bin = os.path.dirname(__file__)
    if _bin not in sys.path:
        sys.path.insert(0, _bin)
    from core.service_catalog import (
        DISPLAY_NAMES as PROVIDER_MAP,
        get_display_name as _catalog_display_name,
        get_internal_priority as _catalog_priority,
    )
    _CATALOG_AVAILABLE = True
except ImportError:
    _CATALOG_AVAILABLE = False
    # Minimal fallback map for scheme-based lookups
    PROVIDER_MAP = {
        'sportsonespn': 'ESPN+', 'sportscenter': 'ESPN+', 'peacock': 'Peacock',
        'peacocktv': 'Peacock', 'pplus': 'Paramount+', 'aiv': 'Prime Video',
        'gametime': 'NBA', 'cbssportsapp': 'CBS Sports', 'cbstve': 'CBS',
        'nbcsportstve': 'NBC Sports', 'foxone': 'FOX Sports (App)', 'fsapp': 'FOX Sports (Alt)',
        'watchtru': 'truTV', 'watchtnt': 'TNT', 'marquee': 'Marquee Sports Network',
        'nesn': 'NESN 360', 'dazn': 'DAZN', 'open.dazn.com': 'DAZN',
        'vixapp': 'ViX', 'f1tv': 'F1 TV', 'nflctv': 'NFL+', 'nflmobile': 'NFL',
        'https': 'Web', 'http': 'Web',
    }

# Legacy list form of priorities, used by filter_playables_by_services sort key.
# Order matches INTERNAL_PRIORITY from service_catalog (lower index = higher priority).
DEFAULT_PROVIDER_PRIORITY = [
    'espn_linear', 'sportsonespn', 'espn_plus', 'sportscenter',
    'peacock', 'peacock_web', 'pplus', 'max',
    'aiv_free', 'aiv_prime', 'aiv_peacock', 'aiv_max',
    'cbssportsapp', 'cbstve', 'nbcsportstve',
    'foxone', 'aiv_fox', 'aiv_fox_one', 'fsapp',
    'apple_mls', 'apple_mlb', 'apple_nba', 'apple_nhl', 'apple_f1', 'apple_other',
    'dazn', 'aiv_dazn', 'open.dazn.com', 'f1tv',
    'kayo_web', 'bein', 'victory', 'nesn', 'nesn_web', 'fanatiz_web',
    'gotham', 'marquee', 'vixapp', 'aiv_vix_premium', 'aiv_vix',
    'aiv_tennis_channel', 'aiv_fanduel', 'nflctv',
    'watchtru', 'ncaa_march_madness', 'watchtnt', 'watchtbs',
    'nba', 'aiv_nba_league_pass', 'gametime', 'mlb', 'nhl', 'nflmobile',
    'aiv', 'aiv_aggregator', 'https', 'http',
]


def extract_provider_from_url(url: str) -> str:
    """
    Extract provider scheme from deeplink URL
    
    Examples:
        sportsonespn://... -> sportsonespn
        pplus://... -> pplus
        https://... -> https
    """
    if not url:
        return 'unknown'
    
    if '://' in url:
        return url.split('://')[0]
    
    return 'unknown'


def get_display_name_from_domain(url: str) -> str:
    """
    Get provider display name by analyzing URL domain.
    Used for web-based services where scheme is https/http.
    
    Examples:
        https://www.victoryplus.com/... -> Victory+
        https://www.gothamsports.com/... -> Gotham Sports
        https://watch.fanatiz.com/... -> Fanatiz Soccer
    """
    if not url:
        return None
    
    # Regional sports networks
    if "victoryplus.com" in url:
        return "Victory+"
    elif "gothamfc.com" in url:
        return "Gotham FC"
    elif "gothamsports.com" in url:
        return "Gotham Sports"
    
    # Premium cable networks
    elif "watch.tbs.com" in url or "watchtbs" in url:
        return "TBS"
    
    # International/regional soccer
    elif "watch.fanatiz.com" in url or "fanatiz.com" in url:
        return "Fanatiz Soccer"
    elif "beinsports.com" in url or "bein" in url.lower():
        return "beIN Sports"
    
    # Regional sports
    elif "watch.nesn.com" in url:
        return "NESN 360"
    
    return None


def get_provider_display_name(provider_scheme: str) -> str:
    """Get human-readable name for a provider scheme."""
    if _CATALOG_AVAILABLE:
        return _catalog_display_name(provider_scheme)
    return PROVIDER_MAP.get(provider_scheme, provider_scheme.upper())


def get_provider_priority(provider_scheme: str) -> int:
    """Get priority for provider (lower = higher priority). Returns 999 if unknown."""
    if _CATALOG_AVAILABLE:
        p = _catalog_priority(provider_scheme)
        return p if p != 25 else 999  # 25 is the catalog default for unknowns
    try:
        return DEFAULT_PROVIDER_PRIORITY.index(provider_scheme)
    except ValueError:
        return 999


def filter_playables_by_services(playables: list, enabled_services: list = None) -> list:
    """
    Filter playables list to only include enabled services
    
    Args:
        playables: List of playable dicts with 'provider' key
        enabled_services: List of enabled provider schemes (None = all enabled)
    
    Returns:
        Filtered list of playables, sorted by priority
    """
    if enabled_services is None or len(enabled_services) == 0:
        # All services enabled - just sort by priority
        result = playables
    else:
        # Filter to only enabled services
        result = [p for p in playables if p.get('provider') in enabled_services]
    
    # Sort by priority
    result.sort(key=lambda p: get_provider_priority(p.get('provider', 'unknown')))
    
    return result


def get_best_deeplink(playables: list, enabled_services: list = None) -> dict:
    """
    Get the best deeplink from a list of playables
    
    Returns the highest priority playable that matches enabled services.
    Returns None if no matches found.
    """
    filtered = filter_playables_by_services(playables, enabled_services)
    
    if not filtered:
        return None
    
    return filtered[0]


def get_all_providers_from_db(conn) -> list:
    """
    Get list of all unique providers in database
    
    Returns list of (provider_scheme, display_name, count) tuples
    """
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT provider, COUNT(*) as count
            FROM playables
            WHERE provider IS NOT NULL AND provider != ''
            GROUP BY provider
            ORDER BY count DESC
        """)
        
        results = []
        for row in cur.fetchall():
            provider = row[0]
            count = row[1]
            display_name = get_provider_display_name(provider)
            results.append((provider, display_name, count))
        
        return results
    except Exception as e:
        # Table might not exist yet
        return []


if __name__ == '__main__':
    # Test the functions
    test_urls = [
        'sportsonespn://...',
        'pplus://www.paramountplus.com/...',
        'peacock://...',
        'aiv://...',
        'https://www.example.com/...',
    ]
    
    print("Provider Extraction Tests:")
    print("="*50)
    for url in test_urls:
        provider = extract_provider_from_url(url)
        display = get_provider_display_name(provider)
        priority = get_provider_priority(provider)
        print(f"{url[:30]:30} -> {provider:15} ({display:15}) [Priority: {priority}]")
