#!/usr/bin/env python3
"""
xmltv_helpers.py - Shared XMLTV tagging and categorization logic

Provides consistent Live/New detection and category taxonomy across
all FruitDeepLinks exporters (lanes, adb_lanes, direct).
"""

import json
from typing import Dict, Optional, List
import xml.etree.ElementTree as ET

# -------------------- Provider Display Names --------------------
def get_provider_display_name(provider_id: str) -> Optional[str]:
    """Map provider IDs to friendly display names"""
    if not provider_id:
        return None
    
    # Try to import and use the logical service mapper first
    try:
        from logical_service_mapper import get_service_display_name
        return get_service_display_name(provider_id)
    except ImportError:
        pass
    
    # Fallback to local mapping if import fails
    provider_lower = provider_id.lower()
    
    provider_map = {
        'sportscenter': 'ESPN+',
        'sportsonespn': 'ESPN+',
        'peacock': 'Peacock',
        'peacocktv': 'Peacock',
        'peacock_web': 'Peacock (Web)',
        'pplus': 'Paramount+',
        'aiv': 'Prime Video',
        'gametime': 'NBA',
        'cbssportsapp': 'CBS Sports',
        'foxone': 'FOX Sports',
        'dazn': 'DAZN',
        'open.dazn.com': 'DAZN',
        'max': 'Max',
        'f1tv': 'F1 TV',
        'apple_mls': 'Apple MLS',
        'apple_mlb': 'Apple MLB',
        'apple_nba': 'Apple NBA',
        'apple_nhl': 'Apple NHL',
        'apple_other': 'Apple TV+',
        'victory': 'Victory+',
        'https': 'Web - Other',
        'http': 'Web - Other',
        'kayo_web': 'Kayo (Web)',
        'kayo': 'Kayo',
    }
    
    return provider_map.get(provider_lower, provider_id.title())


def get_provider_from_channel(channel_name: str) -> str:
    """Extract provider name from channel_name field"""
    if not channel_name:
        return "Sports"
    
    channel_lower = channel_name.lower()
    
    if "espn" in channel_lower:
        return "ESPN+"
    elif "peacock" in channel_lower or "nbc" in channel_lower:
        return "Peacock"
    elif "prime" in channel_lower or "amazon" in channel_lower:
        return "Prime Video"
    elif "cbs" in channel_lower:
        return "CBS Sports"
    elif "paramount" in channel_lower:
        return "Paramount+"
    elif "fox" in channel_lower:
        return "FOX Sports"
    elif "dazn" in channel_lower:
        return "DAZN"
    elif "apple" in channel_lower:
        return "Apple TV+"
    elif "kayo" in channel_lower:
        return "Kayo"
    elif "victory" in channel_lower:
        return "Victory+"
    else:
        return channel_name


# -------------------- Live Detection --------------------
def is_live_broadcast(event: Dict) -> bool:
    """
    Detect if an event is a live broadcast using multiple heuristics.
    
    Checks:
    1. events.airing_type field (if present)
    2. raw_attributes_json for provider-specific live markers
       - Kayo: data.playback.info.playbackType == "LIVE"
       - Apple: isLive fields
    
    Returns True if event is likely a live broadcast, False otherwise.
    """
    # Check airing_type field
    airing_type = event.get("airing_type", "")
    if airing_type and "live" in str(airing_type).lower():
        return True
    
    # Check raw_attributes_json for provider-specific markers
    raw_json = event.get("raw_attributes_json")
    if raw_json:
        try:
            raw = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
            
            # Kayo detection
            if isinstance(raw, dict):
                # Kayo: data.playback.info.playbackType
                playback_type = None
                if "data" in raw:
                    data = raw["data"]
                    if isinstance(data, dict) and "playback" in data:
                        playback = data["playback"]
                        if isinstance(playback, dict) and "info" in playback:
                            info = playback["info"]
                            if isinstance(info, dict):
                                playback_type = info.get("playbackType")
                
                if playback_type and str(playback_type).upper() == "LIVE":
                    return True
                
                # Apple detection: look for any isLive field
                def check_for_is_live(obj):
                    if isinstance(obj, dict):
                        if obj.get("isLive") is True:
                            return True
                        for v in obj.values():
                            if check_for_is_live(v):
                                return True
                    elif isinstance(obj, list):
                        for item in obj:
                            if check_for_is_live(item):
                                return True
                    return False
                
                if check_for_is_live(raw):
                    return True
        except Exception:
            pass
    
    # Default: assume live for sports events
    # (Conservative approach - most sports events are live)
    return True


def is_new_broadcast(event: Dict) -> bool:
    """
    Detect if an event is a new/first-run broadcast.
    
    Currently returns True by default - assumes all sports events are new/first-run.
    
    Future enhancement: check for is_reair field
    - Return True when is_reair == 0
    - Return False when is_reair == 1 (should use <previously-shown/> instead)
    """
    is_reair = event.get("is_reair")
    if is_reair is not None:
        try:
            return int(is_reair) == 0
        except (ValueError, TypeError):
            pass
    
    # Default: assume all sports events are new/first-run broadcasts
    return True


# -------------------- Category Building --------------------
def add_categories_and_tags(
    prog_el: ET.Element,
    event: Dict,
    provider_name: Optional[str] = None,
    is_placeholder: bool = False,
) -> None:
    """
    Add comprehensive XMLTV categories and live/new tags to a programme element.
    
    Args:
        prog_el: The <programme> XML element to modify
        event: Event data dictionary
        provider_name: Display name of the provider (e.g., "ESPN+", "Peacock")
        is_placeholder: If True, skip categories and tags (for idle blocks)
    
    Category taxonomy for real sports events:
    - Provider (e.g., "ESPN+", "Peacock", "Kayo (Web)")
    - "Sports"
    - "Sports Event"
    - Sport (e.g., "Basketball", "Soccer")
    - League (e.g., "NBA", "NHL")
    - Genres from genres_json
    
    Tags:
    - <live/> when is_live_broadcast() returns True
    - <new/> when is_new_broadcast() returns True (currently disabled by default)
    """
    if is_placeholder:
        # No categories or tags for placeholders/idle blocks
        return
    
    # Provider category
    if provider_name:
        ET.SubElement(prog_el, "category").text = provider_name
    
    # Standard sports categories
    ET.SubElement(prog_el, "category").text = "Sports"
    ET.SubElement(prog_el, "category").text = "Sports Event"
    
    # Sport and League from classification_json
    classification_json = event.get("classification_json")
    if classification_json:
        try:
            classification = json.loads(classification_json) if isinstance(classification_json, str) else classification_json
            if isinstance(classification, dict):
                sport = classification.get("sport")
                league = classification.get("league")
                
                if sport and sport != "Sports":
                    ET.SubElement(prog_el, "category").text = str(sport)
                
                if league:
                    ET.SubElement(prog_el, "category").text = str(league)
        except Exception:
            pass
    
    # Additional genres
    genres_json = event.get("genres_json")
    if genres_json:
        try:
            genres = json.loads(genres_json) if isinstance(genres_json, str) else genres_json
            if isinstance(genres, list):
                for g in genres:
                    if not g:
                        continue
                    g_str = str(g)
                    # Skip duplicates of what we've already added
                    if g_str in ("Sports", "Sports Event"):
                        continue
                    if provider_name and g_str == provider_name:
                        continue
                    ET.SubElement(prog_el, "category").text = g_str
        except Exception:
            pass
    
    # Live tag - conditionally based on detection
    if is_live_broadcast(event):
        ET.SubElement(prog_el, "live")
    
    # New tag - conditionally based on detection
    if is_new_broadcast(event):
        ET.SubElement(prog_el, "new")


def get_classification_categories(event: Dict) -> Dict[str, Optional[str]]:
    """
    Extract sport and league from classification_json for display.
    
    Returns:
        dict with 'sport' and 'league' keys (may be None)
    """
    result = {"sport": None, "league": None}
    
    classification_json = event.get("classification_json")
    if not classification_json:
        return result
    
    try:
        classification = json.loads(classification_json) if isinstance(classification_json, str) else classification_json
        
        # Handle both formats:
        # Dict format: {"sport": "Hockey", "league": "NHL"}
        # List format: [{"type": "sport", "value": "Hockey"}, {"type": "league", "value": "NHL"}]
        if isinstance(classification, dict):
            result["sport"] = classification.get("sport")
            result["league"] = classification.get("league")
        elif isinstance(classification, list):
            for item in classification:
                if isinstance(item, dict):
                    item_type = item.get("type")
                    item_value = item.get("value")
                    if item_type in ("sport", "league") and item_value:
                        result[item_type] = item_value
    except Exception:
        pass
    
    return result


# -------------------- Description Enhancement --------------------
def build_enhanced_description(event: Dict, provider_name: Optional[str] = None) -> str:
    """
    Build an ESPN-style enhanced description for XMLTV.
    
    Creates rich descriptions like:
    "Basketball - (Men's College Basketball) - North Carolina Tar Heels vs Georgia Tech Yellow Jackets - Available on ESPN+"
    
    Falls back gracefully when data is missing:
    "Hockey - (WHL) - Edmonton at Swift Current at InnovationPlex - Available on Victory+ (Home Feed)"
    
    Strategy:
    1. Start with synopsis (if rich) or title
    2. Extract and remove feed type (Home/Away Feed) from title/synopsis
    3. Extract sport/league from classification_json
    4. Extract sport detail from genres_json (like "Men's College Basketball")
    5. Build structured description: Sport - (Detail) - Event - Provider (Feed)
    6. Add venue if synopsis was minimal
    
    Args:
        event: Event data dictionary
        provider_name: Display name of the provider (e.g., "ESPN+", "Victory+")
    
    Returns:
        Enhanced description string
    """
    title = event.get("title") or "Sports Event"
    synopsis = event.get("synopsis") or ""
    synopsis_brief = event.get("synopsis_brief") or ""
    venue = event.get("venue") or ""
    
    # Clean synopsis if it contains old formatted text from previous export versions
    # Pattern examples:
    #   "(Basketball) - Basketball - (Men's College Basketball) - Texas Longhorns..."
    #   "(Hockey) - Red Deer at Prince George"
    #   "Basketball - (Men's College Basketball) - ..."
    import re
    if synopsis:
        # Remove patterns like "(Sport) - Sport - (Detail) - " or "Sport - (Detail) - "
        synopsis = re.sub(r'^\([^)]+\)\s*-\s*[^-]+\s*-\s*\([^)]+\)\s*-\s*', '', synopsis)
        synopsis = re.sub(r'^\([^)]+\)\s*-\s*', '', synopsis)  # "(Hockey) - "
        synopsis = re.sub(r'^[^-]+-\s*\([^)]+\)\s*-\s*', '', synopsis)  # "Sport - (Detail) - "
        synopsis = synopsis.strip()
    
    if synopsis_brief:
        synopsis_brief = re.sub(r'^\([^)]+\)\s*-\s*[^-]+\s*-\s*\([^)]+\)\s*-\s*', '', synopsis_brief)
        synopsis_brief = re.sub(r'^\([^)]+\)\s*-\s*', '', synopsis_brief)
        synopsis_brief = re.sub(r'^[^-]+-\s*\([^)]+\)\s*-\s*', '', synopsis_brief)
        synopsis_brief = synopsis_brief.strip()
    
    # Strip any existing "Available on X" suffix
    synopsis = re.sub(r'\s*-\s*Available on [^-]+$', '', synopsis)
    synopsis_brief = re.sub(r'\s*-\s*Available on [^-]+$', '', synopsis_brief)
    
    # Extract and remove feed type from title/synopsis (Home Feed, Away Feed, etc.)
    feed_type = None
    feed_pattern = r'\s*-\s*(Home Feed|Away Feed|National Feed|Local Feed|Main Feed|Alternate Feed)$'
    
    # Check title for feed suffix
    title_match = re.search(feed_pattern, title, re.IGNORECASE)
    if title_match:
        feed_type = title_match.group(1)
        title = re.sub(feed_pattern, '', title, flags=re.IGNORECASE)
    
    # Check synopsis for feed suffix
    if not feed_type:
        synopsis_match = re.search(feed_pattern, synopsis, re.IGNORECASE)
        if synopsis_match:
            feed_type = synopsis_match.group(1)
            synopsis = re.sub(feed_pattern, '', synopsis, flags=re.IGNORECASE)
    
    # Clean up any trailing whitespace/dashes
    title = title.strip().rstrip('-').strip()
    synopsis = synopsis.strip().rstrip('-').strip()
    synopsis_brief = synopsis_brief.strip().rstrip('-').strip()
    
    # Get classification (sport/league)
    classification = get_classification_categories(event)
    sport = classification.get("sport")
    league = classification.get("league")
    
    # Get sport detail from genres_json (like "Men's College Basketball")
    sport_detail = None
    genres_json = event.get("genres_json")
    if genres_json:
        try:
            genres = json.loads(genres_json) if isinstance(genres_json, str) else genres_json
            if isinstance(genres, list):
                # Look for detailed sport genres (longer than basic sport name)
                # Don't filter out the sport itself - we want sport OUTSIDE and detail INSIDE parens
                candidates = [
                    g for g in genres 
                    if g and isinstance(g, str) and g not in ("Sports", "Sports Event")
                    and (not sport or g != sport)  # Skip if same as sport
                    and (not league or g != league)  # Skip if same as league
                    and (not provider_name or g != provider_name)  # Skip provider names
                ]
                # Pick the longest/most specific genre as detail (or use league if no detail)
                if candidates:
                    sport_detail = max(candidates, key=len)
        except Exception:
            pass
    
    # Determine base description
    # Priority: synopsis (if substantial) > synopsis_brief > title
    base_desc = synopsis if synopsis and len(synopsis) > 30 else (synopsis_brief or title)
    
    # If base description is just the venue, enhance it with title
    if venue and base_desc.strip() == venue.strip():
        base_desc = f"{title} at {venue}"
    elif venue and len(base_desc) < 40 and venue not in base_desc:
        # Synopsis is minimal and doesn't include venue - add it
        base_desc = f"{base_desc} at {venue}" if base_desc != title else f"{title} at {venue}"
    
    # Build structured description parts
    parts = []
    
    # Part 1: Sport (always first if available)
    if sport and sport != "Sports":
        parts.append(sport)
    
    # Part 2: Sport detail or league (in parentheses)
    # Priority: sport_detail (if different from sport) > league
    detail_part = None
    if sport_detail and sport_detail != sport:
        # Use sport_detail only if it's different from sport (e.g., "Men's College Basketball")
        detail_part = sport_detail
    elif league:
        # Otherwise use league (e.g., "WHL", "NBA")
        detail_part = league
    
    if detail_part:
        parts.append(f"({detail_part})")
    
    # Part 3: Event description
    parts.append(base_desc)
    
    # Part 4: Provider (with optional feed type)
    if provider_name:
        if feed_type:
            parts.append(f"Available on {provider_name} ({feed_type})")
        else:
            parts.append(f"Available on {provider_name}")
    
    # Join with " - " separator (ESPN style)
    return " - ".join(parts)
