#!/usr/bin/env python3
"""
Export XMLTV + M3U with STABLE channel ids that match M3U tvg-id.

Updates:
- Placeholder times (upcoming/ended) are rendered in system local time with tz abbrev.
- Stable XML <channel id> == M3U tvg-id via fdl.<event_id|pvid>
- Deterministic SQL ordering
- 24h default window, placeholders, provider categories, image extraction, deeplinks
- FRUIT_DIRECT_START_CH environment variable for custom M3U channel numbering (default 5000)
"""

from __future__ import annotations

import os
import argparse
import json
import sqlite3
import urllib.parse
import sys
import re
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import filtering support
try:
    from filter_integration import (
        load_user_preferences,
        should_include_event,
        get_best_deeplink_for_event,
        get_best_playable_for_event,
        get_all_deeplinks_for_event,
        get_fallback_deeplink,
        expand_enabled_services_for_amazon,
    )

    FILTERING_AVAILABLE = True
except ImportError:
    print("Warning: filter_integration not available, filtering disabled")
    FILTERING_AVAILABLE = False

    def load_user_preferences(conn):
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": [], "team_rules": []}

    def should_include_event(event, prefs):
        return True

    def get_best_deeplink_for_event(conn, event_id, services):
        return None

    def get_best_playable_for_event(conn, event_id, services, *args, **kwargs):
        return None

    def get_all_deeplinks_for_event(conn, event_id, services, *args, **kwargs):
        return []

    def get_fallback_deeplink(event):
        return None

    def expand_enabled_services_for_amazon(conn, enabled_services):
        return enabled_services


try:
    from db.preferences import get_setting
except ImportError:
    def get_setting(conn, key, fallback=None):
        return fallback


# Import shared XMLTV helpers
try:
    from xmltv_helpers import (
        build_enhanced_description,
        build_enhanced_title,
        get_base_service_label,
        get_service_label_for_playable,
        label_playables_for_expand,
    )
except ImportError:
    # Fallback if not in path
    def get_base_service_label(playable, fallback="Sports"):
        return (playable or {}).get("service_name") or fallback

    def build_enhanced_title(event):
        import re
        # Get title - never use synopsis/description
        title = event.get("title")
        
        # Fallback if title is missing or empty
        if not title or not title.strip():
            # Use channel_name as last resort, never synopsis
            title = event.get("channel_name") or "Sports Event"
        
        # Clean feed suffixes
        feed_pattern = r'\s*-\s*(Home Feed|Away Feed|National Feed|Local Feed|Main Feed|Alternate Feed)$'
        title = re.sub(feed_pattern, '', title, flags=re.IGNORECASE)
        return title.strip().rstrip('-').strip()
    
    def build_enhanced_description(event, provider_name=None):
        import re
        synopsis = event.get("synopsis") or event.get("synopsis_brief") or event.get("title") or "Sports Event"
        
        # Clean polluted synopsis
        if synopsis:
            synopsis = re.sub(r'^\([^)]+\)\s*-\s*[^-]+\s*-\s*\([^)]+\)\s*-\s*', '', synopsis)
            synopsis = re.sub(r'^\([^)]+\)\s*-\s*', '', synopsis)
            synopsis = re.sub(r'^[^-]+-\s*\([^)]+\)\s*-\s*', '', synopsis)
            synopsis = re.sub(r'\s*-\s*Available on [^-]+$', '', synopsis)
            synopsis = synopsis.strip()
        
        if provider_name:
            return f"{synopsis} - Available on {provider_name}"
        return synopsis

    def get_service_label_for_playable(playable, fallback="Sports", event_title=None):
        return playable.get("service_name") or fallback

    def label_playables_for_expand(all_playables, fallback, event_title=None):
        return [(p, get_service_label_for_playable(p, fallback=fallback), fallback) for p in all_playables]


# -------------------- Deprecated Services --------------------
# Services that have been removed and should be filtered out from preferences
DEPRECATED_SERVICES = {
    "aiv_exclusive",  # Removed: Amazon Exclusives synthetic provider (replaced by direct Amazon scraping)
}


def filter_deprecated_services(services: List[str]) -> List[str]:
    """
    Remove deprecated services from a service list.
    Silently filters out any services in DEPRECATED_SERVICES.
    """
    if not services:
        return services
    return [s for s in services if s not in DEPRECATED_SERVICES]


# -------------------- DB Helpers --------------------
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def check_tables(conn: sqlite3.Connection, needed: List[str]) -> Tuple[bool, List[str]]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    have = {r[0] for r in cur.fetchall()}
    missing = [t for t in needed if t not in have]
    return (len(missing) == 0, missing)


# -------------------- Time helpers --------------------
def xmltv_time(dt: datetime) -> str:
    """
    XMLTV time format: YYYYMMDDHHMMSS +0000 (UTC)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M%S +0000")


def parse_iso(s: str) -> datetime:
    # Stored as ISO 8601 in UTC
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def snap_to_half_hour(dt: datetime) -> datetime:
    """
    Snap dt up to the next :00 or :30.
    Used to align placeholder blocks on nice boundaries.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    minutes = dt.minute
    if minutes == 0 or minutes == 30:
        snapped = dt.replace(second=0, microsecond=0)
    elif minutes < 30:
        snapped = dt.replace(minute=30, second=0, microsecond=0)
    else:
        snapped = (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return snapped


# -------------------- Channel / provider helpers --------------------
def _sanitize_id(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", ".", s)
    if not s:
        s = "ev"
    if s[0].isdigit():
        s = "x" + s
    return s


def stable_channel_id(event: Dict, prefix: str = "fdl.") -> str:
    key = event.get("id") or event.get("event_id") or event.get("pvid")
    if key:
        return _sanitize_id(prefix + key)
    t = (event.get("title") or "event").strip()
    st = (event.get("start_utc") or "").replace("-", "").replace(":", "").replace(
        "T", ""
    ).replace("Z", "")
    return _sanitize_id(prefix + t + "." + st)


def stable_channel_id_for_playable(event: Dict, playable_id: Optional[str], prefix: str = "fdl.") -> str:
    """Like stable_channel_id(), but unique per playable -- used in "expand all
    playables" mode where one event maps to several channels, one per source."""
    base = stable_channel_id(event, prefix)
    if not playable_id:
        return base
    return _sanitize_id(base + "." + playable_id)


def get_provider_from_channel(channel_name: str) -> str:
    if not channel_name:
        return "Sports"

    cl = channel_name.lower()
    if "espn" in cl:
        return "ESPN+"
    if "peacock" in cl:
        return "Peacock"
    if "national broadcasting company" in cl or channel_name == "National Broadcasting Company":
        return "Peacock"
    if "nbc sports" in cl:
        return "NBC Sports"
    if "prime" in cl or "amazon" in cl:
        return "Prime Video"
    if "cbs" in cl:
        return "CBS Sports"
    if "paramount" in cl:
        return "Paramount+"
    if "fox" in cl:
        return "FOX Sports"
    if "nfl" in cl and "network" not in cl:
        return "NFL+"
    if "nba" in cl and "tv" not in cl:
        return "NBA League Pass"
    if "mlb" in cl and "tv" not in cl:
        return "MLB.TV"
    if "nhl" in cl and "network" not in cl:
        return "NHL Power Play"
    if "hbo" in cl or "max" in cl:
        return "Max"
    if "dazn" in cl:
        return "DAZN"
    return "Sports"


# Local time display helpers
_LOCAL_TZ = datetime.now().astimezone().tzinfo


def _fmt_local_short(dt_utc: datetime) -> str:
    """
    Human-friendly local time for placeholders. Example: 'Sun at 04:00 AM EST'
    """
    ldt = dt_utc.astimezone(_LOCAL_TZ)
    tz = ldt.strftime("%Z") or ldt.strftime("%z")
    return ldt.strftime(f"%a at %I:%M %p {tz}")


# -------------------- Image helper --------------------
def get_event_image_url(conn: sqlite3.Connection, event: Dict) -> Optional[str]:
    """
    Get the canonical hero image URL from the events table.
    
    This image was pre-selected during import using the best available source:
      1. Versus-style 'gen/...Sports.TVAPo...' (shelfItemImagePost)
      2. Live tile (shelfItemImageLive)
      3. Logo fallback (shelfImageLogo)
    
    All images are normalized to 1280x720 jpg format.
    
    Fallback to event_images table for legacy ESPN events if needed.
    """
    event_id = event.get("id") or event.get("event_id")
    if not event_id:
        return None
    
    # Primary: use hero_image_url from events table
    hero_url = event.get("hero_image_url")
    if hero_url:
        return hero_url
    
    # Fallback: check event_images table for legacy events
    cur = conn.cursor()
    cur.execute(
        "SELECT url FROM event_images WHERE event_id=? ORDER BY img_type LIMIT 1",
        (event_id,),
    )
    row = cur.fetchone()
    if row and row["url"]:
        return row["url"]
    
    return None
# -------------------- Event selection (24h) --------------------
def get_direct_events(
    conn: sqlite3.Connection,
    hours_window: int = 24,
    apply_filters: bool = True,
    log_summary: bool = False,
) -> List[Dict]:
    """Get events for direct export, optionally applying user filters"""
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(hours=hours_window)
    cur.execute(
        """
        SELECT e.id, e.pvid, e.slug, e.title, e.channel_name,
               e.synopsis, e.synopsis_brief, e.genres_json, e.classification_json,
               e.start_utc, e.end_utc, e.raw_attributes_json, e.hero_image_url
          FROM events e
         WHERE e.start_utc <= ?
           AND e.end_utc   >= ?
         ORDER BY e.start_utc ASC,
                  e.end_utc ASC,
                  e.title ASC,
                  e.id ASC
        """,
        (window_end.isoformat(), now.isoformat()),
    )
    all_events = [dict(row) for row in cur.fetchall()]

    # Apply content filters if enabled
    if apply_filters and FILTERING_AVAILABLE:
        preferences = load_user_preferences(conn)
        
        # Filter out deprecated services from enabled_services
        if "enabled_services" in preferences and preferences["enabled_services"]:
            preferences["enabled_services"] = filter_deprecated_services(preferences["enabled_services"])
        
        filtered_events: List[Dict] = []
        for event in all_events:
            if should_include_event(event, preferences):
                filtered_events.append(event)

        if log_summary:
            kept = len(filtered_events)
            total = len(all_events)
            removed = total - kept

            # Optional: breakdown by logical service if mapper is available
            try:
                from logical_service_mapper import get_all_logical_services_with_counts

                service_counts = get_all_logical_services_with_counts(conn)
                all_services = sorted(service_counts.keys())
                enabled_services = preferences.get("enabled_services", [])
                # Expand 'aiv' and normalize aliases so display matches what filtering uses
                enabled_services = expand_enabled_services_for_amazon(conn, enabled_services)
                # If Amazon master toggle is OFF, remove all aiv* from display
                # (they're in enabled_services but blocked at filter time by master toggle)
                amazon_master_enabled = preferences.get("amazon_master_enabled", True)
                if not amazon_master_enabled:
                    enabled_services = [s for s in enabled_services
                                        if s != "aiv" and not s.startswith("aiv_")]
                if enabled_services:
                    disabled_services = [s for s in all_services if s not in enabled_services]
                else:
                    # enabled_services == [] means ALL are allowed
                    disabled_services = []
            except Exception:
                # If logical_service_mapper is unavailable, skip disabled-services breakdown
                enabled_services = preferences.get("enabled_services", [])
                disabled_services = []

            print("  Filter settings:")
            print(
                "    Enabled services ({count}): {items}".format(
                    count=len(enabled_services),
                    items=", ".join(sorted(enabled_services)) if enabled_services else "ALL",
                )
            )
            if disabled_services:
                print(
                    "    Disabled services ({count}): {items}".format(
                        count=len(disabled_services),
                        items=", ".join(disabled_services),
                    )
                )
            else:
                print("    Disabled services (0): None")
            print(
                "    Events kept: {kept} / {total} (removed {removed})".format(
                    kept=kept,
                    total=total,
                    removed=removed,
                )
            )

        return filtered_events

    # If filters were requested but filter_integration isn't available, say so once
    if log_summary and apply_filters and not FILTERING_AVAILABLE:
        print("  Filter settings: filtering requested but filter_integration is not available")

    return all_events


# -------------------- Direct XMLTV --------------------
def _add_direct_xmltv_channel(
    tv: ET.Element,
    conn: sqlite3.Connection,
    event: Dict,
    chan_id: str,
    title: str,
    provider: str,
    now: datetime,
    max_event_minutes: int,
) -> None:
    """Emit one <channel> + its placeholder/main/placeholder <programme> blocks.

    Shared by the single-"best"-pick path and the expand-all-playables path in
    build_direct_xmltv() -- everything here is per-channel, not per-deeplink,
    so both paths call it once per emitted channel.
    """
    chan = ET.SubElement(tv, "channel", id=chan_id)
    dn = ET.SubElement(chan, "display-name")
    dn.text = title

    event_start = parse_iso(event["start_utc"])
    event_end = parse_iso(event["end_utc"])
    if event_end <= event_start:
        event_end = event_start + timedelta(hours=3)

    # Cap runaway event lengths (e.g. bad upstream end_utc showing an
    # MLB game as 8+ hours) to the user's configured maximum.
    if max_event_minutes > 0:
        max_end = event_start + timedelta(minutes=max_event_minutes)
        if event_end > max_end:
            event_end = max_end

    # Pre-event placeholders (from now-1h snapped to :00/:30)
    pre_start = snap_to_half_hour(now - timedelta(hours=1))
    current = pre_start
    while current < event_start:
        block_end = min(current + timedelta(hours=1), event_start)
        if (block_end - current).total_seconds() < 60:
            break
        pre_prog = ET.SubElement(
            tv,
            "programme",
            channel=chan_id,
            start=xmltv_time(current),
            stop=xmltv_time(block_end),
        )
        ET.SubElement(pre_prog, "title").text = "Event Not Started"
        ET.SubElement(pre_prog, "desc").text = (
            f"Starts { _fmt_local_short(event_start) }. Available on {provider}."
        )
        current = block_end

    # Main event
    prog = ET.SubElement(
        tv,
        "programme",
        channel=chan_id,
        start=xmltv_time(event_start),
        stop=xmltv_time(event_end),
    )
    ET.SubElement(prog, "title").text = title

    # Build enhanced description (ESPN-style)
    desc_text = build_enhanced_description(event, provider_name=provider)
    ET.SubElement(prog, "desc").text = desc_text

    # Categories
    ET.SubElement(prog, "category").text = provider
    ET.SubElement(prog, "category").text = "Sports"
    genres_json = event.get("genres_json")
    if genres_json:
        try:
            for g in json.loads(genres_json) or []:
                if g and g not in (provider, "Sports"):
                    ET.SubElement(prog, "category").text = str(g)
        except Exception:
            pass

    # Attach image to main event
    img_url = get_event_image_url(conn, event)
    if img_url:
        ET.SubElement(prog, "icon", src=img_url)

    # Only mark as live if it's truly live or a premiere (not a replay)
    # Replays should not be marked as live content
    airing_type = event.get('airing_type')
    if airing_type not in ('replay',):
        ET.SubElement(prog, "live").text = "1"

    # Post-event placeholders (24h in 1h blocks)
    current = event_end
    post_end = event_end + timedelta(hours=24)
    while current < post_end:
        block_end = min(current + timedelta(hours=1), post_end)
        post_prog = ET.SubElement(
            tv,
            "programme",
            channel=chan_id,
            start=xmltv_time(current),
            stop=xmltv_time(block_end),
        )
        ET.SubElement(post_prog, "title").text = "Event Ended"
        ET.SubElement(post_prog, "desc").text = (
            f"Ended { _fmt_local_short(event_end) }. Available on {provider}."
        )
        current = block_end


def build_direct_xmltv(
    conn: sqlite3.Connection,
    xml_path: str,
    hours_window: int = 24,
    epg_prefix: str = "fdl.",
    apply_filters: bool = True,
):
    events = get_direct_events(
        conn, hours_window=hours_window, apply_filters=apply_filters, log_summary=True
    )
    print(f"Direct XMLTV: {len(events)} event channels (within {hours_window}h)")

    # Load user preferences for deeplink selection
    preferences = load_user_preferences(conn) if FILTERING_AVAILABLE else {}

    # Filter out deprecated services and expand 'aiv' -> all aiv_* sub-services
    enabled_services = filter_deprecated_services(preferences.get("enabled_services", []))
    enabled_services = expand_enabled_services_for_amazon(conn, enabled_services)

    priority_map = preferences.get("service_priorities", {})
    amazon_penalty = preferences.get("amazon_penalty", True)
    language_preference = preferences.get("language_preference", "en")
    amazon_master_enabled = preferences.get("amazon_master_enabled", True)
    expand_all = bool(get_setting(conn, "expand_all_playables", False))
    max_event_minutes = int(get_setting(conn, "max_event_minutes", 0) or 0)

    now = datetime.now(timezone.utc)
    tv = ET.Element("tv")
    tv.set("generator-info-name", "FruitDeepLinks - Direct")
    tv.set("generator-info-url", "https://github.com/yourusername/FruitDeepLinks")
    cur = conn.cursor()

    channels_emitted = 0

    for event in events:
        title = build_enhanced_title(event)
        channel_name = event.get("channel_name") or "Sports"
        event_id = event.get("id", "")

        if expand_all and FILTERING_AVAILABLE:
            all_playables = get_all_deeplinks_for_event(
                conn, event_id, enabled_services, priority_map, amazon_penalty,
                language_preference, amazon_master_enabled,
            )
            if not all_playables:
                continue
            for playable, service_label, group_label in label_playables_for_expand(all_playables, fallback=get_provider_from_channel(channel_name) or "Sports", event_title=title):
                chan_id = stable_channel_id_for_playable(event, playable.get("playable_id"), epg_prefix)
                chan_title = f"{title} ({service_label})"
                _add_direct_xmltv_channel(tv, conn, event, chan_id, chan_title, group_label, now, max_event_minutes)
                channels_emitted += 1
            continue

        chan_id = stable_channel_id(event, epg_prefix)

        # Get deeplink URL + provider label from the SAME winning playable --
        # not re-derived from the deeplink's URL scheme afterward, which
        # collapsed every espn_* variant (Unlimited/Plus/Linear/MLB.TV/MLB
        # Network) to the same generic "ESPN+" label since they all share
        # the sportscenter:// scheme regardless of actual logical_service.
        # get_best_deeplink_for_event() already applies the ESPN Graph ID
        # swap for the specific winning playable internally, so no separate
        # correction pass is needed here.
        deeplink_url = None
        provider = None
        if FILTERING_AVAILABLE:
            best_playable = get_best_playable_for_event(
                conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference
            )
            if best_playable:
                deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference)
                provider = get_base_service_label(best_playable, fallback=None)

        if not deeplink_url and FILTERING_AVAILABLE:
            # Fallback to raw_attributes
            deeplink_url = get_fallback_deeplink(event)

        if not deeplink_url:
            # Final fallback for Peacock events
            pvid = event.get("pvid")
            if pvid and not event_id.startswith("appletv-"):
                payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
                deeplink_url = "https://www.peacocktv.com/deeplink?deeplinkData=" + urllib.parse.quote(
                    json.dumps(payload, separators=(",", ":"), ensure_ascii=False), safe=""
                )

        # Final fallback: use database channel_name provider
        if not provider:
            provider = get_provider_from_channel(channel_name) or "Sports"

        _add_direct_xmltv_channel(tv, conn, event, chan_id, title, provider, now, max_event_minutes)
        channels_emitted += 1

    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    Path(xml_path).parent.mkdir(parents=True, exist_ok=True)
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote Direct XMLTV: {xml_path} ({channels_emitted} channels{' -- expand all playables ON' if expand_all else ''})")


# -------------------- M3U --------------------
def build_direct_m3u(
    conn: sqlite3.Connection,
    m3u_path: str,
    hours_window: int = 24,
    epg_prefix: str = "fdl.",
    apply_filters: bool = True,
):
    events = get_direct_events(conn, hours_window=hours_window, apply_filters=apply_filters)
    print(f"Direct M3U: {len(events)} event channels (within {hours_window}h)")

    preferences = load_user_preferences(conn) if FILTERING_AVAILABLE else {}

    # Filter out deprecated services and expand 'aiv' -> all aiv_* sub-services
    enabled_services = filter_deprecated_services(preferences.get("enabled_services", []))
    enabled_services = expand_enabled_services_for_amazon(conn, enabled_services)

    priority_map = preferences.get("service_priorities", {})
    amazon_penalty = preferences.get("amazon_penalty", True)
    language_preference = preferences.get("language_preference", "en")
    amazon_master_enabled = preferences.get("amazon_master_enabled", True)
    expand_all = bool(get_setting(conn, "expand_all_playables", False))

    skipped_no_deeplink = 0
    reason_counts: Dict[str, int] = {}
    service_skip_counts: Dict[str, int] = {}

    def bump(reason: str) -> None:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

    def bump_service(service: str) -> None:
        service_skip_counts[service] = service_skip_counts.get(service, 0) + 1

    cur = conn.cursor()

    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        # Starting channel number: DB setting wins, then env var, then default (get_setting handles the fallback chain).
        # `is None` (not truthy-`or`) so an explicit setting of 0 isn't discarded.
        direct_start_ch = get_setting(conn, "direct_start_ch")
        if direct_start_ch is None:
            direct_start_ch = int(os.getenv("FRUIT_DIRECT_START_CH", "5000"))
        idx = direct_start_ch
        for event in events:
            pvid = event.get("pvid")
            if not pvid:
                continue

            title = event.get("title") or f"Sports Event {idx}"
            channel_name = event.get("channel_name") or "Sports"
            provider = get_provider_from_channel(channel_name)

            img_url = get_event_image_url(conn, event)

            event_id = event.get("id", "")

            if expand_all and FILTERING_AVAILABLE:
                all_playables = get_all_deeplinks_for_event(
                    conn, event_id, enabled_services, priority_map, amazon_penalty,
                    language_preference, amazon_master_enabled,
                )
                if not all_playables:
                    bump("no_url_for_any_service")
                    skipped_no_deeplink += 1
                    continue
                for playable, service_label, group_label in label_playables_for_expand(all_playables, fallback=provider or "Sports", event_title=title):
                    chan_id = stable_channel_id_for_playable(event, playable.get("playable_id"), epg_prefix)
                    entry_title = f"{title} ({service_label})"
                    logo_part = f'tvg-logo="{img_url}" ' if img_url else ""
                    f.write(
                        '#EXTINF:-1 tvg-id="{id}" tvg-name="{name}" tvg-chno="{chno}" {logo}group-title="{group}",{name}\n'.format(
                            id=chan_id,
                            name=entry_title.replace(",", " "),
                            chno=idx,
                            logo=logo_part,
                            group=group_label.replace('"', "'"),
                        )
                    )
                    f.write(f"{playable['resolved_deeplink']}\n\n")
                    idx += 1
                continue

            chan_id = stable_channel_id(event, epg_prefix)
            deeplink_url = None
            reason = None

            try:
                cur.execute(
                    """SELECT provider, playable_url, deeplink_play, deeplink_open, priority, espn_graph_id, logical_service
                           FROM playables
                           WHERE event_id = ?""",
                    (event_id,),
                )
                p_rows = cur.fetchall()
            except Exception:
                p_rows = []

            # Winning playable dict (not just its deeplink string) so the
            # group-title below can use its actual logical_service instead of
            # re-deriving a label from the deeplink's URL scheme afterward --
            # every espn_* variant (Unlimited/Plus/Linear/MLB.TV/MLB Network)
            # shares the sportscenter:// scheme, so that re-derivation always
            # collapsed to the same generic "ESPN+" regardless of which one
            # actually won.
            best_playable = get_best_playable_for_event(
                conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference
            ) if FILTERING_AVAILABLE else None

            if FILTERING_AVAILABLE and p_rows:
                # get_best_deeplink_for_event() already resolves the ESPN Watch
                # Graph correction internally (via _resolve_deeplink_for_playable(),
                # using the SAME winning playable's own espn_graph_id) -- no
                # separate "fix" pass needed here. A prior version of this function
                # had two such passes: one that scanned ALL of this event's
                # playables (ignoring enabled_services entirely) for "any" ESPN
                # graph ID to splice in, and a second fallback pass reimplementing
                # the same filtering logic by hand. Both were silent no-ops in
                # practice -- the first's `espn_graph_id.split(':')` check assumed
                # the legacy "espn-watch:{id}:{hash}" format and never matched
                # today's bare-UUID storage, and the second crashed on
                # `sqlite3.Row.get()` (Row has no .get() method) and was swallowed
                # by its own try/except -- but if either had ever actually fired,
                # it could have silently substituted a DIFFERENT playable's stream
                # (including one excluded by the user's filters) for the correctly
                # selected one. Removed rather than fixed: get_filtered_playables()
                # already does everything the second pass was trying to do.
                deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference)

            if not deeplink_url:
                has_playables = bool(p_rows)
                has_raw_url = False
                raw_url_fallback = None  # Initialize the variable

                raw = event.get("raw_attributes_json")
                if raw:
                    try:
                        data = json.loads(raw)
                        candidate = data.get("webUrl") or data.get("web_url") or data.get("url")
                        if isinstance(candidate, str) and candidate.startswith("http"):
                            has_raw_url = True
                            raw_url_fallback = candidate  # Save the URL for fallback use
                    except Exception:
                        pass

                if has_playables:
                    if FILTERING_AVAILABLE and enabled_services:
                        reason = "playables_filtered_out"
                    else:
                        reason = "playables_no_deeplink"
                elif not has_playables and has_raw_url:
                    reason = "raw_attributes_only"
                else:
                    reason = "no_playables_no_rawattrs"

                if raw_url_fallback:
                    deeplink_url = raw_url_fallback
                    reason = None

            if not deeplink_url:
                if reason is None:
                    reason = "no_url_for_any_service"
                bump(reason)
                skipped_no_deeplink += 1
                continue

            # Determine actual provider label. Prefer the winning playable's
            # own logical_service (correct even when multiple ESPN entitlement
            # tiers share one URL scheme) -- only fall back to re-deriving
            # from the deeplink URL for paths that never resolved a playable
            # dict at all (raw_attributes-only / Peacock fallback events).
            actual_provider = get_base_service_label(best_playable, fallback=None) if best_playable else None

            if not actual_provider and FILTERING_AVAILABLE:
                try:
                    from logical_service_mapper import get_service_display_name, get_logical_service_for_playable
                    from provider_utils import extract_provider_from_url, get_provider_display_name, get_display_name_from_domain

                    # Extract provider from the actual deeplink URL scheme
                    scheme = extract_provider_from_url(deeplink_url)
                    if scheme and scheme not in ("http", "https", "unknown"):
                        # For non-web deeplinks, get the display name
                        actual_provider = get_provider_display_name(scheme)
                        # If not found, try logical service mapper
                        if actual_provider == scheme.upper():
                            try:
                                logical_service = get_logical_service_for_playable(
                                    provider=scheme,
                                    deeplink_play=deeplink_url,
                                    deeplink_open=None,
                                    playable_url=None,
                                    event_id=event_id,
                                    conn=conn,
                                )
                                actual_provider = get_service_display_name(logical_service)
                            except:
                                pass
                    elif scheme in ("http", "https"):
                        # For web URLs, check the domain
                        actual_provider = get_display_name_from_domain(deeplink_url)
                        if not actual_provider:
                            actual_provider = "Web"
                except Exception as e:
                    # If detection fails, we'll use fallback below
                    pass
            
            # Fallback: use database channel_name provider only if detection completely failed
            if not actual_provider:
                actual_provider = provider

            if reason:
                bump(reason)

            logo_part = f'tvg-logo="{img_url}" ' if img_url else ""
            group_title = actual_provider or provider or "Sports"
            f.write(
                '#EXTINF:-1 tvg-id="{id}" tvg-name="{name}" tvg-chno="{chno}" {logo}group-title="{group}",{name}\n'.format(
                    id=chan_id,
                    name=title.replace(",", " "),
                    chno=idx,
                    logo=logo_part,
                    group=group_title.replace('"', "'"),
                )
            )
            f.write(f"{deeplink_url}\n\n")
            idx += 1

    print(f"Wrote Direct M3U: {m3u_path}{' (expand all playables ON)' if expand_all else ''}")
    if skipped_no_deeplink:
        print(f"  Skipped {skipped_no_deeplink} events with no usable deeplink")
        print("  Skip reasons:")
        for k, v in sorted(reason_counts.items()):
            print(f"    {k}: {v}")
    if service_skip_counts:
        print("  Service skip counts:")
        for k, v in sorted(service_skip_counts.items()):
            print(f"    {k}: {v}")


# -------------------- Stubs for lanes (unchanged) --------------------
def build_adbtuner_xmltv(conn, xml_path):
    print("Skipping lanes XMLTV - use full version")


def build_adbtuner_m3u(conn, m3u_path, server_url):
    print("Skipping lanes M3U - use full version")


def build_chrome_m3u(conn, m3u_path, server_url):
    print("Skipping chrome M3U - use full version")


# -------------------- CLI --------------------
def main():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent if script_dir.name == "bin" else script_dir

    default_db = str(repo_root / "data" / "fruit_events.db")
    default_direct_xml = str(repo_root / "out" / "direct.xml")
    default_direct_m3u = str(repo_root / "out" / "direct.m3u")

    ap = argparse.ArgumentParser(description="Export FruitDeepLinks direct XMLTV + M3U")
    ap.add_argument("--db", default=default_db, help=f"SQLite DB path (default: {default_db})")
    ap.add_argument(
        "--direct-xml",
        default=default_direct_xml,
        help=f"Output XMLTV path (default: {default_direct_xml})",
    )
    ap.add_argument(
        "--direct-m3u",
        default=default_direct_m3u,
        help=f"Output M3U path (default: {default_direct_m3u})",
    )
    ap.add_argument(
        "--hours-window",
        type=int,
        default=24,
        help="Hours ahead to include (default: 24)",
    )
    ap.add_argument(
        "--epg-prefix",
        default="fdl.",
        help="Prefix for stable channel ids (default: fdl.)",
    )
    ap.add_argument(
        "--no-filters",
        action="store_true",
        help="Disable user content filters",
    )

    args = ap.parse_args()
    apply_filters = not args.no_filters

    print("FruitDeepLinks Direct Export")
    print("============================")
    print(f"DB: {args.db}")
    print(f"XMLTV: {args.direct_xml}")
    print(f"M3U: {args.direct_m3u}")
    print(f"Hours window: {args.hours_window}")
    print(f"EPG prefix: {args.epg_prefix}")
    print(f"Filtering: {'ENABLED' if apply_filters else 'DISABLED'}")
    if apply_filters and not FILTERING_AVAILABLE:
        print("  Warning: filter_integration.py not found, filtering disabled")
    print()

    conn = get_conn(args.db)
    ok, missing = check_tables(conn, ["events"])
    if not ok:
        print(f"ERROR: Missing tables: {', '.join(missing)}")
        return 1

    build_direct_xmltv(
        conn,
        args.direct_xml,
        hours_window=args.hours_window,
        epg_prefix=args.epg_prefix,
        apply_filters=apply_filters,
    )
    build_direct_m3u(
        conn,
        args.direct_m3u,
        hours_window=args.hours_window,
        epg_prefix=args.epg_prefix,
        apply_filters=apply_filters,
    )

    conn.close()
    print("\nExport complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
