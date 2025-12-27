#!/usr/bin/env python3
"""
deeplink_converter.py - Convert app-scheme deeplinks to HTTP URLs (Android/Fire TV compatibility)

FruitDeepLinks reality check (from your scrape DBs):
  - Some providers already hand back HTTPS (keep as-is)
  - Some are simple scheme->https rewrites (pplus, cbstve, open.dazn.com)
  - Some need parameter extraction (aiv gti=..., ESPN playID=...)
  - ESPN has a special case: Apple provides channel-based deeplinks (playChannel=espn1),
    but the *watch playback UUID* is embedded in playable_id like:
      tvs.sbd.30061:<UUID>:<suffix>
    We can deterministically turn that into:
      https://www.espn.com/watch/player/_/id/<UUID>

Design goals:
  - Pure string parsing: no network calls.
  - Return an HTTPS URL when we can; otherwise return None (caller can fall back to original).
  - Keep backward compatible signature (provider hint optional).

Usage:
  http_url = generate_http_deeplink(
      punchout_url,
      provider=provider_hint,       # optional
      playable_id=playable_id,      # optional (IMPORTANT for ESPN playChannel case)
      league=league_hint,           # optional (helps CBS Sports)
      context={"vix_locale":"es-es"}# optional
  )
"""

from __future__ import annotations

import re
from typing import Optional, Dict, Any
from urllib.parse import urlparse, parse_qs


# ----------------------------
# Helpers
# ----------------------------

_SCHEME_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9+.\-]*):")

def _scheme(url: str) -> str:
    m = _SCHEME_RE.match(url or "")
    return m.group(1).lower() if m else ""

def _slugify(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[’']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


# ----------------------------
# Provider converters
# ----------------------------

def convert_amazon_prime(punchout_url: str) -> Optional[str]:
    """
    aiv://aiv/detail?gti=<GTI>&action=watch&type=live...
      -> https://app.primevideo.com/detail?gti=<GTI>
    """
    if not punchout_url or not punchout_url.lower().startswith("aiv://"):
        return None
    pr = urlparse(punchout_url)
    qs = parse_qs(pr.query)
    gti = (qs.get("gti") or [None])[0]
    if gti:
        return f"https://app.primevideo.com/detail?gti={gti}"
    return None


def convert_espn(punchout_url: str, playable_id: Optional[str] = None, espn_graph_id: Optional[str] = None) -> Optional[str]:
    """
    ESPN (SportsCenter scheme)

    Priority (best to worst):
      1. espn_graph_id from ESPN Watch Graph API (most reliable for ADBTuner)
      2. playID from sportscenter:// URL
      3. playable_id extraction from tvs.sbd pattern
      4. Fallback to ESPN Watch landing page

    espn_graph_id format: "espn-watch:{playID}:{hash}"
    Example: "espn-watch:9eb9b68b-11c6-4da0-9492-df997dbbf897:bb816546..."
    We extract the middle part (the actual playID).

    Case A (ESPN Graph ID available - BEST):
      espn_graph_id="espn-watch:9eb9b68b...:hash"
        -> https://www.espn.com/watch/player/_/id/9eb9b68b...

    Case B (event-level playID in URL):
      sportscenter://x-callback-url/showWatchStream?playID=<UUID>&...
        -> https://www.espn.com/watch/player/_/id/<UUID>

    Case C (channel-based deeplink, no playID):
      sportscenter://...showWatchStream?playChannel=espn1
        -> Extract playback UUID from playable_id: tvs.sbd.30061:<UUID>:...
        -> https://www.espn.com/watch/player/_/id/<UUID>

    Otherwise:
      -> https://www.espn.com/watch/ (landing)
    """
    # Priority 1: Use ESPN Graph ID if available (from ESPN Watch Graph enrichment)
    if espn_graph_id:
        # Format: espn-watch:{playID}:{hash}
        # Extract the middle part
        parts = espn_graph_id.split(':')
        if len(parts) >= 2:
            play_id = parts[1]
            if _UUID_RE.match(play_id):
                return f"https://www.espn.com/watch/player/_/id/{play_id}"
    
    if not punchout_url or not punchout_url.lower().startswith("sportscenter://"):
        return None

    pr = urlparse(punchout_url)
    qs = parse_qs(pr.query)

    # Priority 2: Check for playID in URL
    play_id = (qs.get("playID") or qs.get("playId") or qs.get("playid") or [None])[0]
    if play_id and _UUID_RE.match(play_id):
        return f"https://www.espn.com/watch/player/_/id/{play_id}"

    # Priority 3: Channel-based - try to pull UUID from playable_id
    if playable_id:
        # Common Apple pattern: tvs.sbd.30061:<UUID>:<suffix>
        m = re.search(r":([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):", playable_id, re.I)
        if m:
            return f"https://www.espn.com/watch/player/_/id/{m.group(1)}"

    return "https://www.espn.com/watch/"


def convert_paramount_plus(punchout_url: str) -> Optional[str]:
    """
    pplus://www.paramountplus.com/live-tv/stream/<slug>/<uuid>/
      -> https://www.paramountplus.com/live-tv/stream/<slug>/<uuid>/
    """
    if not punchout_url or not punchout_url.lower().startswith("pplus://"):
        return None
    return "https://" + punchout_url.split("://", 1)[1].lstrip("/")


def convert_cbs_tve(punchout_url: str) -> Optional[str]:
    """
    cbstve://www.cbs.com/live-tv/stream/sports/<uuid>/
      -> https://www.cbs.com/live-tv/stream/sports/<uuid>/
    """
    if not punchout_url or not punchout_url.lower().startswith("cbstve://"):
        return None
    return "https://" + punchout_url.split("://", 1)[1].lstrip("/")


def convert_dazn(punchout_url: str) -> Optional[str]:
    """
    open.dazn.com://media/open/<id>
      -> https://open.dazn.com/media/open/<id>
    """
    if not punchout_url:
        return None
    if punchout_url.lower().startswith("open.dazn.com://"):
        return "https://open.dazn.com/" + punchout_url.split("://", 1)[1].lstrip("/")
    return None


def convert_vix(punchout_url: str, locale: str = "es-es") -> Optional[str]:
    """
    vixapp://live/transmission-matchid-XXXX?play
      -> https://vix.com/<locale>/live/transmission-matchid-XXXX?play
    """
    if not punchout_url or not punchout_url.lower().startswith("vixapp://"):
        return None
    tail = punchout_url.split("://", 1)[1].lstrip("/")
    if not tail.startswith("live/"):
        tail = "live/" + tail
    return f"https://vix.com/{locale}/" + tail


def convert_fox(punchout_url: str) -> Optional[str]:
    """
    fsapp://live/FS1?eventId=... -> https://www.foxsports.com/live/fs1?eventId=...
    foxone://channel/fs1         -> https://www.foxsports.com/live/fs1
    """
    if not punchout_url:
        return None
    u = punchout_url.lower()

    if u.startswith("fsapp://live/"):
        tail = punchout_url.split("fsapp://live/", 1)[1]
        parts = tail.split("?", 1)
        channel = parts[0].strip("/").lower()
        q = ("?" + parts[1]) if len(parts) > 1 else ""
        return f"https://www.foxsports.com/live/{channel}{q}"

    if u.startswith("foxone://channel/"):
        channel = punchout_url.split("foxone://channel/", 1)[1].strip("/").lower()
        return f"https://www.foxsports.com/live/{channel}"

    return None


def convert_turner(punchout_url: str) -> Optional[str]:
    """
    watchtnt://play?... -> https://www.tntdrama.com/watchtnt?...
    watchtru://play?... -> https://www.trutv.com/watchtrutv?...
    """
    if not punchout_url:
        return None
    u = punchout_url.lower()
    if u.startswith("watchtnt://play"):
        suffix = punchout_url.split("watchtnt://play", 1)[1]
        return "https://www.tntdrama.com/watchtnt" + suffix
    if u.startswith("watchtru://play"):
        suffix = punchout_url.split("watchtru://play", 1)[1]
        return "https://www.trutv.com/watchtrutv" + suffix
    return None


def convert_nba_gametime(punchout_url: str) -> Optional[str]:
    """
    NBA (gametime) deeplink - strip Apple TV query params
    
    Input:  gametime://game/0022500409?x-source=umc.ums.apple.tvapp&x-apple-...
    Output: gametime://game/0022500409
    
    Per user report from CDVR forum (2024-12):
    "First test only opened NBA app. Then scrubbed query string linking it to atv 
    and it opened app, and set it to the event page of the game."
    
    Keep the native gametime:// scheme but remove Apple TV tracking params.
    This works better when launching from CDVR on Fire TV / Android devices.
    """
    if not punchout_url or not punchout_url.lower().startswith("gametime://"):
        return None

    # Strip query string parameters - just keep the base deeplink
    if '?' in punchout_url:
        return punchout_url.split('?')[0]
    
    # Already clean
    return punchout_url


def convert_nbcsports(punchout_url: str) -> Optional[str]:
    """
    NBC Sports TVE (nbcsportstve)

    Apple punchout examples:
      nbcsportstve://watch/12013522

    We tested the naive rewrite:
      https://www.nbcsports.com/watch/12013522
    and it returns 404. So for Android/Fire TV HTTP compatibility, we fall back to the
    NBC Sports Watch schedule hub (works in browser):

      https://www.nbcsports.com/watch/schedule
    """
    if not punchout_url or not punchout_url.lower().startswith("nbcsportstve://"):
        return None
    return "https://www.nbcsports.com/watch/schedule"


def convert_nfl_ctv(punchout_url: str) -> Optional[str]:
    """
    nflctv://livestream/<uuid> -> no stable public event-level URL identified yet.
    Fallback to NFL+ landing.
    """
    if not punchout_url or not punchout_url.lower().startswith("nflctv://"):
        return None
    return "https://www.nfl.com/plus/"


# CBS Sports app needs a league-ish path segment to form the most specific watch URL.
_CBS_LEAGUE_TO_PATH = {
    "Men's College Basketball": "college-basketball",
    "Women's College Basketball": "womens-college-basketball",
    "Conference League": "uefa-conference-league",
    "Women's Champions League": "uefa-womens-champions-league",
    "EFL Cup": "carabao-cup",
    "EFL Championship": "efl",
    "England League One": "efl",
    "England League Two": "efl",
    "Scottish Premiership": "scottish-professional-football-league",
    "Serie A": "serie-a",
    "Italy Supercoppa Italiana": "serie-a",
    "Major Arena Soccer League": "soccer",
}

def convert_cbssports(punchout_url: str, league: Optional[str] = None) -> Optional[str]:
    """
    cbssportsapp://home/watch/LET-211531296?source=tvapp
      -> https://www.cbssports.com/watch/<path>/<LET-...>

    If league is unknown:
      -> https://www.cbssports.com/watch/LET-...
    """
    if not punchout_url or not punchout_url.lower().startswith("cbssportsapp://"):
        return None
    m = re.search(r"/watch/(LET-\d+)", punchout_url)
    if not m:
        return None
    let_id = m.group(1)
    if not league:
        return f"https://www.cbssports.com/watch/{let_id}"
    path = _CBS_LEAGUE_TO_PATH.get(league) or _slugify(league)
    return f"https://www.cbssports.com/watch/{path}/{let_id}"


def convert_peacock(punchout_url: str) -> Optional[str]:
    """
    (Still a best guess / not validated against the latest scrape DB)
    peacock://event/<id> -> https://www.peacocktv.com/watch/playback/event/<id>
    """
    if not punchout_url or not punchout_url.lower().startswith("peacock://"):
        return None
    if punchout_url.lower().startswith("peacock://event/"):
        event_id = punchout_url.split("peacock://event/", 1)[1]
        return f"https://www.peacocktv.com/watch/playback/event/{event_id}"
    return None


# ----------------------------
# Public API
# ----------------------------

def generate_http_deeplink(
    punchout_url: str,
    provider: Optional[str] = None,
    playable_id: Optional[str] = None,
    league: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    espn_graph_id: Optional[str] = None,
) -> Optional[str]:
    """
    Convert app-scheme deeplinks to HTTP URLs.

    Args:
        punchout_url: The deeplink URL to convert
        provider: Provider hint (optional)
        playable_id: Playable ID (optional, used for ESPN extraction)
        league: League hint (optional, used for CBS Sports)
        context: Additional context (optional)
        espn_graph_id: ESPN Watch Graph ID (optional, preferred for ESPN)

    Returns:
      - HTTPS URL if conversion is possible (or punchout_url itself if already HTTPS)
      - None if no conversion is available (caller may keep original scheme)
    """
    if not punchout_url:
        return None

    # Already HTTPS? keep it.
    if re.match(r"^https?://", punchout_url, re.I):
        return punchout_url

    prov = (provider or _scheme(punchout_url) or "").lower()
    ctx = context or {}

    # Route by scheme/provider
    if prov in ("aiv", "amazon prime video", "prime video"):
        return convert_amazon_prime(punchout_url)

    if prov in ("sportscenter", "espn", "espn+"):
        return convert_espn(punchout_url, playable_id=playable_id, espn_graph_id=espn_graph_id)

    if prov in ("pplus", "paramount", "paramount+"):
        return convert_paramount_plus(punchout_url)

    if prov in ("cbstve", "cbs"):
        return convert_cbs_tve(punchout_url)

    if prov in ("open.dazn.com", "dazn"):
        return convert_dazn(punchout_url)

    if prov in ("vixapp", "vix"):
        return convert_vix(punchout_url, locale=ctx.get("vix_locale", "es-es"))

    if prov in ("fsapp", "foxone", "fox sports"):
        return convert_fox(punchout_url)

    if prov in ("watchtnt", "watchtru"):
        return convert_turner(punchout_url)

    if prov in ("gametime", "nba"):
        return convert_nba_gametime(punchout_url)

    if prov in ("nbcsportstve", "nbcsports"):
        return convert_nbcsports(punchout_url)

    if prov in ("cbssportsapp", "cbs sports"):
        league_hint = league or ctx.get("league")
        return convert_cbssports(punchout_url, league=league_hint)

    if prov in ("nflctv", "nfl"):
        return convert_nfl_ctv(punchout_url)

    # Last resort: scheme://www.domain/... -> https://www.domain/...
    m = re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://(www\.[^/]+/.+)$", punchout_url)
    if m:
        return "https://" + m.group(1)

    return None


def generate_espn_scheme_deeplink(espn_graph_id: Optional[str] = None, fallback_url: Optional[str] = None) -> Optional[str]:
    """
    Generate a working sportscenter:// deeplink for ADBTuner using ESPN Watch Graph ID.
    
    This fixes the Apple TV deeplink format that uses playChannel (doesn't work) 
    and converts it to playID format (works with ADBTuner/ESPN4cc4c).
    
    Args:
        espn_graph_id: ESPN Watch Graph ID from enrichment (espn-watch:{playID}:{hash})
        fallback_url: Original Apple TV deeplink to use if espn_graph_id not available
        
    Returns:
        sportscenter://x-callback-url/showWatchStream?playID={playID}
        or fallback_url if espn_graph_id is not available
        
    Examples:
        Input:  espn_graph_id = "espn-watch:9eb9b68b-11c6-4da0-9492-df997dbbf897:bb816546..."
        Output: "sportscenter://x-callback-url/showWatchStream?playID=9eb9b68b-11c6-4da0-9492-df997dbbf897"
    """
    if not espn_graph_id:
        return fallback_url
    
    # Extract playID from ESPN Graph ID format: espn-watch:{playID}:{hash}
    parts = espn_graph_id.split(':')
    if len(parts) >= 2:
        play_id = parts[1]
        if _UUID_RE.match(play_id):
            return f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"
    
    # ESPN Graph ID format unexpected, use fallback
    return fallback_url


if __name__ == "__main__":
    # Minimal smoke tests
    tests = [
        ("aiv://aiv/detail?gti=amzn1.dv.gti.10fd272d-309e-427a-87b6-6289003e2ccb&action=watch&type=live",
         dict(provider="aiv"),
         "https://app.primevideo.com/detail?gti=amzn1.dv.gti.10fd272d-309e-427a-87b6-6289003e2ccb"),

        ("sportscenter://x-callback-url/showWatchStream?playID=3be751ec-31ee-466d-9d5a-59645ee401aa&x-source=AppleUMC",
         dict(provider="sportscenter"),
         "https://www.espn.com/watch/player/_/id/3be751ec-31ee-466d-9d5a-59645ee401aa"),

        # ESPN playChannel case needs playable_id
        ("sportscenter://x-callback-url/showWatchStream?playChannel=espn1&x-source=AppleUMC",
         dict(provider="sportscenter", playable_id="tvs.sbd.30061:21a4067c-1db2-4cfa-8b6c-e8c339b32047:4050e1f9"),
         "https://www.espn.com/watch/player/_/id/21a4067c-1db2-4cfa-8b6c-e8c339b32047"),

        ("pplus://www.paramountplus.com/live-tv/stream/serie-a/49f986ec-3ab2-44d7-ade6-6dfd2df5b492/",
         dict(provider="pplus"),
         "https://www.paramountplus.com/live-tv/stream/serie-a/49f986ec-3ab2-44d7-ade6-6dfd2df5b492/"),

        ("cbstve://www.cbs.com/live-tv/stream/sports/046fb39f-9eda-4968-adde-c0162f566980/",
         dict(provider="cbstve"),
         "https://www.cbs.com/live-tv/stream/sports/046fb39f-9eda-4968-adde-c0162f566980/"),

        ("open.dazn.com://media/open/74d3bc02-dc0b-4060-8d79-c9eb3b103461",
         dict(provider="open.dazn.com"),
         "https://open.dazn.com/media/open/74d3bc02-dc0b-4060-8d79-c9eb3b103461"),

        ("vixapp://live/transmission-matchid-LGUA25065?play",
         dict(provider="vixapp"),
         "https://vix.com/es-es/live/transmission-matchid-LGUA25065?play"),

        ("fsapp://live/FS1?eventId=undefined&headerTitle=FOX+Sports+Live&sport=undefined",
         dict(provider="fsapp"),
         "https://www.foxsports.com/live/fs1?eventId=undefined&headerTitle=FOX+Sports+Live&sport=undefined"),

        ("watchtnt://play?stream=east&appId=27125",
         dict(provider="watchtnt"),
         "https://www.tntdrama.com/watchtnt?stream=east&appId=27125"),

                ("nbcsportstve://watch/12013522",
         dict(provider="nbcsportstve"),
         "https://www.nbcsports.com/watch/schedule"),

("gametime://game/0022500373?source=atv-search",
         dict(provider="gametime"),
         "https://www.nba.com/game/0022500373"),

        ("cbssportsapp://home/watch/LET-211531296?source=tvapp",
         dict(provider="cbssportsapp", league="Serie A"),
         "https://www.cbssports.com/watch/serie-a/LET-211531296"),

        ("nflctv://livestream/f8d8eae6-311e-11f0-b670-ae1250fadad1",
         dict(provider="nflctv"),
         "https://www.nfl.com/plus/"),
        
        # ESPN with Graph ID (new enrichment feature)
        ("sportscenter://x-callback-url/showWatchStream?playChannel=espn1&x-source=AppleUMC",
         dict(provider="sportscenter", espn_graph_id="espn-watch:9eb9b68b-11c6-4da0-9492-df997dbbf897:bb816546ee4e3a967b98e9d775c9c6f3"),
         "https://www.espn.com/watch/player/_/id/9eb9b68b-11c6-4da0-9492-df997dbbf897"),
    ]

    ok = 0
    for u, kwargs, expected in tests:
        got = generate_http_deeplink(u, **kwargs)
        status = "✓" if got == expected else "✗"
        print(f"{status} {u}\n  expected: {expected}\n  got:      {got}\n")
        ok += (got == expected)

    print(f"Passed {ok}/{len(tests)} tests")
    
    # Test ESPN scheme deeplink generation
    print("\n" + "="*60)
    print("ESPN Scheme Deeplink Generation Tests")
    print("="*60)
    
    espn_graph_id = "espn-watch:9eb9b68b-11c6-4da0-9492-df997dbbf897:bb816546ee4e3a967b98e9d775c9c6f3"
    expected_scheme = "sportscenter://x-callback-url/showWatchStream?playID=9eb9b68b-11c6-4da0-9492-df997dbbf897"
    got_scheme = generate_espn_scheme_deeplink(espn_graph_id)
    
    print(f"{'✓' if got_scheme == expected_scheme else '✗'} ESPN Graph ID → Scheme")
    print(f"  Input:    {espn_graph_id}")
    print(f"  Expected: {expected_scheme}")
    print(f"  Got:      {got_scheme}\n")
    
    # Test fallback
    fallback_url = "sportscenter://x-callback-url/showWatchStream?playChannel=espn1"
    got_fallback = generate_espn_scheme_deeplink(None, fallback_url)
    print(f"{'✓' if got_fallback == fallback_url else '✗'} Fallback when no Graph ID")
    print(f"  Got:      {got_fallback}")

