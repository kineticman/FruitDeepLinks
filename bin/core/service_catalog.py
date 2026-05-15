#!/usr/bin/env python3
"""
service_catalog.py - Single source of truth for streaming service metadata

All service codes, display names, and priorities are defined here.
Other modules import from this file rather than maintaining their own copies.

Priority scales:
  INTERNAL_PRIORITY: 0-31, lower = higher priority (programmatic selection)
  DEFAULT_USER_PRIORITY: 1-100, higher = more preferred (user-facing defaults)
"""

# ---------------------------------------------------------------------------
# Service Display Names
# Maps logical service codes -> human-readable names
# ---------------------------------------------------------------------------
DISPLAY_NAMES: dict[str, str] = {
    # ESPN family
    "sportsonespn": "ESPN+",
    "sportscenter": "ESPN+",
    "espn_linear": "ESPN (Linear)",
    "espn_plus": "ESPN+",

    # Peacock / NBC
    "peacock": "Peacock",
    "peacocktv": "Peacock",
    "peacock_web": "Peacock (Web)",
    "nbcsportstve": "NBC Sports",

    # Paramount / CBS
    "pplus": "Paramount+",
    "cbssportsapp": "CBS Sports",
    "cbstve": "CBS",

    # Amazon family
    "aiv": "Prime Video",
    "aiv_prime": "Amazon - Prime Exclusive",
    "aiv_peacock": "Amazon - Peacock",
    "aiv_max": "Amazon - Max",
    "aiv_fox": "Amazon - FOX One",        # legacy code
    "aiv_fox_one": "Amazon - FOX One",    # canonical code
    "aiv_nba_league_pass": "Amazon - NBA League Pass",
    "aiv_wnba_league_pass": "Amazon - WNBA League Pass",
    "aiv_dazn": "Amazon - DAZN",
    "aiv_vix_premium": "Amazon - ViX Premium",
    "aiv_vix": "Amazon - ViX",
    "aiv_tennis_channel": "Amazon - Tennis Channel",
    "aiv_fanduel": "Amazon - FanDuel Sports Network",
    "aiv_paramount_plus": "Amazon - Paramount+",
    "aiv_willow": "Amazon - Willow TV",
    "aiv_free": "Amazon - Free with Ads",
    "aiv_squash": "Amazon - SquashTV",
    "aiv_aggregator": "Amazon - Unknown",

    # Max / Warner
    "max": "Max",

    # NBA / league apps
    "gametime": "NBA",
    "nba": "NBA League Pass",
    "mlb": "MLB.TV",
    "nhl": "NHL.TV",
    "nflctv": "NFL+",
    "nflmobile": "NFL",
    "ncaa_march_madness": "NCAA March Madness",

    # Fox family
    "foxone": "FOX Sports (App)",
    "fsapp": "FOX Sports (Alt)",

    # Turner
    "watchtru": "truTV",
    "watchtnt": "TNT",
    "watchtbs": "TBS",

    # Apple TV+
    "apple_mls": "Apple MLS",
    "apple_mlb": "Apple MLB",
    "apple_nba": "Apple NBA",
    "apple_nhl": "Apple NHL",
    "apple_f1": "Formula 1 (Apple TV)",
    "apple_other": "Apple TV+",

    # Niche / regional
    "dazn": "DAZN",
    "open.dazn.com": "DAZN",
    "vixapp": "ViX",
    "f1tv": "F1 TV",
    "kayo_web": "Kayo Sports",
    "fanatiz_web": "Fanatiz Soccer",
    "bein": "beIN Sports",
    "nesn": "NESN 360",
    "nesn_web": "NESN 360",
    "victory": "Victory+",
    "gotham": "Gotham Sports",
    "marquee": "Marquee Sports Network",

    # Generic web
    "https": "Web - Other",
    "http": "Web - Other",
}


def get_display_name(service_code: str) -> str:
    """Return human-readable name for a service code, falling back to uppercased code."""
    return DISPLAY_NAMES.get(service_code, service_code.upper())


# ---------------------------------------------------------------------------
# Internal Priorities
# Lower number = higher priority.  Used by deeplink selector.
# ---------------------------------------------------------------------------
INTERNAL_PRIORITY: dict[str, int] = {
    # ESPN
    "espn_linear": 0,
    "sportsonespn": 1,
    "espn_plus": 1,
    "sportscenter": 1,

    # Peacock / NBC
    "peacock": 2,
    "peacock_web": 3,

    # Paramount+ / CBS
    "pplus": 4,

    # Max
    "max": 5,

    # Amazon — free / well-matched content
    "aiv_free": 1,
    "aiv_prime": 4,
    "aiv_peacock": 5,
    "aiv_max": 5,

    # Sports channels
    "cbssportsapp": 6,
    "cbstve": 7,
    "nbcsportstve": 8,
    "foxone": 9,
    "aiv_fox": 9,
    "aiv_fox_one": 9,
    "fsapp": 10,

    # Apple services
    "apple_mls": 11,
    "apple_mlb": 12,
    "apple_nba": 13,
    "apple_nhl": 14,
    "apple_f1": 15,
    "apple_other": 16,

    # Niche / specialty
    "dazn": 16,
    "aiv_dazn": 16,
    "open.dazn.com": 17,
    "f1tv": 18,
    "kayo_web": 19,
    "bein": 19,
    "victory": 19,
    "nesn": 19,
    "nesn_web": 19,
    "fanatiz_web": 20,
    "gotham": 20,
    "marquee": 20,
    "vixapp": 21,
    "aiv_vix_premium": 21,
    "aiv_vix": 21,
    "aiv_tennis_channel": 22,
    "aiv_fanduel": 22,
    "nflctv": 22,
    "watchtru": 23,
    "ncaa_march_madness": 24,
    "watchtnt": 24,
    "watchtbs": 25,

    # League passes
    "nba": 26,
    "aiv_nba_league_pass": 26,
    "gametime": 26,
    "mlb": 26,
    "nhl": 26,
    "nflmobile": 27,

    # Amazon aggregator / unknown
    "aiv": 27,
    "aiv_aggregator": 27,

    # Generic web
    "https": 30,
    "http": 31,
}


def get_internal_priority(service_code: str) -> int:
    """Return internal selection priority (lower = better). Defaults to 25."""
    return INTERNAL_PRIORITY.get(service_code, 25)


# ---------------------------------------------------------------------------
# Default User Priorities
# Higher number = more preferred.  Stored in user_preferences and shown in
# the Filters UI.  Users can customize these; these are the factory defaults.
# ---------------------------------------------------------------------------
DEFAULT_USER_PRIORITY: dict[str, int] = {
    # Tier 1: Premium sports services (90-100)
    "sportsonespn": 100,
    "espn_plus": 100,
    "espn_linear": 99,
    "peacock": 98,
    "peacock_web": 98,
    "pplus": 96,
    "max": 94,
    "apple_mls": 92,
    "apple_mlb": 92,
    "apple_f1": 92,
    "apple_nba": 91,
    "apple_nhl": 91,
    "apple_other": 90,

    # Tier 2: Cable / network sports (70-89)
    "watchtnt": 88,
    "watchtru": 87,
    "watchtbs": 86,
    "foxone": 85,
    "fsapp": 84,
    "nbcsportstve": 82,
    "cbssportsapp": 81,
    "cbstve": 80,

    # Tier 3: League-specific services (50-69)
    "nba": 68,
    "nhl": 67,
    "mlb": 66,
    "f1tv": 65,
    "dazn": 64,
    "kayo_web": 63,
    "bein": 63,
    "fanatiz_web": 62,
    "victory": 62,
    "gotham": 62,
    "nesn": 61,
    "nesn_web": 61,
    "ncaa_march_madness": 60,

    # Tier 4: League / sports apps (40-59)
    "gametime": 55,
    "nflctv": 52,
    "nflmobile": 50,
    "marquee": 50,

    # Tier 5: Amazon services (10-29)
    "aiv_free": 28,
    "aiv_prime": 25,
    "aiv_peacock": 22,
    "aiv_max": 22,
    "aiv_fox_one": 20,
    "aiv_fox": 20,
    "aiv_nba_league_pass": 18,
    "aiv_dazn": 18,
    "aiv_vix_premium": 16,
    "aiv_vix": 15,
    "aiv_tennis_channel": 14,
    "aiv_fanduel": 14,
    "aiv_paramount_plus": 13,
    "aiv_willow": 12,
    "aiv_wnba_league_pass": 12,
    "aiv_squash": 11,
    "aiv": 10,
    "aiv_aggregator": 5,

    # Tier 6: Generic / fallback (1-9)
    "https": 5,
    "http": 4,
}


def get_default_user_priority(service_code: str) -> int:
    """Return default user-facing priority (higher = more preferred). Defaults to 15."""
    return DEFAULT_USER_PRIORITY.get(service_code, 15)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def all_service_codes() -> list[str]:
    """Return sorted list of all known service codes."""
    codes = set(DISPLAY_NAMES) | set(INTERNAL_PRIORITY) | set(DEFAULT_USER_PRIORITY)
    return sorted(codes)
