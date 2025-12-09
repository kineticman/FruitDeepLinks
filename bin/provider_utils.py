#!/usr/bin/env python3
"""
provider_utils.py - Utilities for handling streaming provider deeplinks

Extracts provider from URL schemes and provides human-readable names.
"""

# Provider mapping: URL scheme -> Display name
PROVIDER_MAP = {
    # ESPN
    'sportsonespn': 'ESPN+',
    'sportscenter': 'ESPN',
    
    # CBS/Paramount
    'pplus': 'Paramount+',
    'cbssportsapp': 'CBS Sports',
    'cbstve': 'CBS',
    
    # NBC/Peacock
    'peacock': 'Peacock',
    'peacocktv': 'Peacock',
    'nbcsportstve': 'NBC Sports',
    
    # Amazon
    'aiv': 'Prime Video',
    'gametime': 'Prime Video',
    
    # Fox
    'foxone': 'FOX Sports (App)',
    'fsapp': 'FOX Sports (Alt)',
    'watchtru': 'truTV',
    'watchtnt': 'TNT',
    
    # Other
    'dazn': 'DAZN',
    'open.dazn.com': 'DAZN',
    'vixapp': 'ViX',
    'f1tv': 'F1 TV',
    'nflctv': 'NFL+',
    'nflmobile': 'NFL',
    'videos': 'Apple TV+',
    
    # Web fallbacks
    'https': 'Web',
    'http': 'Web',
}

# Provider priority (lower = higher priority)
# Users can customize this in settings
DEFAULT_PROVIDER_PRIORITY = [
    'sportsonespn',     # ESPN+ (usually best quality)
    'peacock',          # Peacock
    'peacocktv',        # Peacock alt
    'pplus',            # Paramount+
    'aiv',              # Prime Video
    'cbssportsapp',     # CBS Sports
    'nbcsportstve',     # NBC Sports
    'foxone',           # FOX Sports
    'fsapp',            # FOX Sports alt
    'dazn',             # DAZN
    'vixapp',           # ViX
    'f1tv',             # F1 TV
    'nflctv',           # NFL+
    'videos',           # Apple TV+
    'cbstve',           # CBS
    'gametime',         # Prime Video (TNF)
    'watchtnt',         # TNT
    'watchtru',         # truTV
    'nflmobile',        # NFL
    'sportscenter',     # ESPN (less reliable)
    'https',            # Web fallback
    'http',             # Web fallback
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


def get_provider_display_name(provider_scheme: str) -> str:
    """
    Get human-readable name for provider
    
    Examples:
        sportsonespn -> ESPN+
        pplus -> Paramount+
        peacock -> Peacock
    """
    return PROVIDER_MAP.get(provider_scheme, provider_scheme.upper())


def get_provider_priority(provider_scheme: str) -> int:
    """
    Get priority value for provider (lower = higher priority)
    Returns 999 if not in priority list
    """
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
