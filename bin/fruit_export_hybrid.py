#!/usr/bin/env python3
"""
Export XMLTV + M3U with STABLE channel ids that match M3U tvg-id.

Updates:
- Placeholder times (upcoming/ended) are rendered in system local time with tz abbrev.
- Stable XML <channel id> == M3U tvg-id via fdl.<event_id|pvid>
- Deterministic SQL ordering
- 24h default window, placeholders, provider categories, image extraction, deeplinks
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
        get_fallback_deeplink,
    )

    FILTERING_AVAILABLE = True
except ImportError:
    print("Warning: filter_integration not available, filtering disabled")
    FILTERING_AVAILABLE = False

    def load_user_preferences(conn):
        return {"enabled_services": [], "disabled_sports": [], "disabled_leagues": []}

    def should_include_event(event, prefs):
        return True

    def get_best_deeplink_for_event(conn, event_id, services):
        return None

    def get_fallback_deeplink(event):
        return None


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
    enabled_services = preferences.get("enabled_services", [])
    priority_map = preferences.get("service_priorities", {})
    amazon_penalty = preferences.get("amazon_penalty", True)
    language_preference = preferences.get("language_preference", "en")

    now = datetime.now(timezone.utc)
    tv = ET.Element("tv")
    tv.set("generator-info-name", "FruitDeepLinks - Direct")
    tv.set("generator-info-url", "https://github.com/yourusername/FruitDeepLinks")

    for idx, event in enumerate(events, start=1):
        chan_id = stable_channel_id(event, epg_prefix)
        title = event.get("title") or f"Sports Event {idx}"
        channel_name = event.get("channel_name") or "Sports"
        event_id = event.get("id", "")

        # Get deeplink URL using similar logic as M3U
        deeplink_url = None
        if FILTERING_AVAILABLE:
            # Try filtered playables first
            deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference)
            
            # ESPN FIX: Apply ESPN Graph ID correction to XMLTV path too
            if deeplink_url and deeplink_url.startswith("sportscenter://"):
                try:
                    cur.execute(
                        """SELECT provider, espn_graph_id
                           FROM playables
                           WHERE event_id = ?""",
                        (event_id,),
                    )
                    for prow in cur.fetchall():
                        # Handle sqlite3.Row objects (use dict access, not .get())
                        raw_provider = prow["provider"] if prow["provider"] else ""
                        if raw_provider.lower() not in ('sportscenter', 'espn', 'espn+'):
                            continue
                        
                        # sqlite3.Row uses dict-style access
                        espn_graph_id = prow["espn_graph_id"] if "espn_graph_id" in prow.keys() else None
                        if espn_graph_id:
                            parts = espn_graph_id.split(':')
                            if len(parts) >= 2:
                                play_id = parts[1]
                                deeplink_url = f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"
                                break
                except Exception:
                    pass

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

        # Determine human-readable provider for XML
        # Start from channel-based guess, but prefer logical_service_mapper when available.
        provider = get_provider_from_channel(channel_name) or "Sports"
        
        # Try to determine provider from best available playable, even if filtered out
        if FILTERING_AVAILABLE:
            try:
                from logical_service_mapper import (
                    get_logical_service_for_playable,
                    get_service_display_name,
                )
                from provider_utils import extract_provider_from_url
                
                # If we have a deeplink, use it
                if deeplink_url:
                    raw_provider = extract_provider_from_url(deeplink_url) or ""
                    playable_url = deeplink_url if deeplink_url.startswith("http") else None
                    
                    logical_service = get_logical_service_for_playable(
                        provider=raw_provider,
                        deeplink_play=deeplink_url,
                        deeplink_open=None,
                        playable_url=playable_url,
                        event_id=event_id,
                        conn=conn,
                    )
                    if logical_service:
                        provider = get_service_display_name(logical_service)
                else:
                    # No deeplink, but check playables for provider metadata
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT provider, deeplink_play, deeplink_open, playable_url, priority, espn_graph_id
                        FROM playables
                        WHERE event_id = ?
                        ORDER BY priority DESC
                        LIMIT 1
                    """, (event_id,))
                    playable = cur.fetchone()
                    
                    if playable:
                        raw_provider = (playable["provider"] or "").strip()
                        deeplink_play = (playable["deeplink_play"] or "").strip()
                        deeplink_open = (playable["deeplink_open"] or "").strip()
                        playable_url_str = (playable["playable_url"] or "").strip()
                        espn_graph_id = (playable.get("espn_graph_id") or "").strip() if "espn_graph_id" in playable.keys() else ""
                        
                        # ESPN FIX: Prefer ESPN Graph ID for ESPN playables
                        if espn_graph_id and raw_provider.lower() in ('sportscenter', 'espn', 'espn+'):
                            try:
                                parts = espn_graph_id.split(':')
                                if len(parts) >= 2:
                                    play_id = parts[1]
                                    deeplink_play = f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"
                            except Exception:
                                pass
                        
                        logical_service = get_logical_service_for_playable(
                            provider=raw_provider,
                            deeplink_play=deeplink_play or None,
                            deeplink_open=deeplink_open or None,
                            playable_url=playable_url_str or None,
                            event_id=event_id,
                            conn=conn,
                        )
                        if logical_service:
                            provider = get_service_display_name(logical_service)
            except Exception as e:
                # Fall back to channel-based provider if mapping fails
                provider = provider or "Sports"

        # Channel element
        chan = ET.SubElement(tv, "channel", id=chan_id)
        dn = ET.SubElement(chan, "display-name")
        dn.text = title

        event_start = parse_iso(event["start_utc"])
        event_end = parse_iso(event["end_utc"])
        if event_end <= event_start:
            event_end = event_start + timedelta(hours=3)

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

        base_desc = event.get("synopsis") or event.get("synopsis_brief") or title
        sport_label = None
        genres_json = event.get("genres_json")
        if genres_json:
            try:
                genres = json.loads(genres_json)
                if isinstance(genres, list):
                    cands = [g for g in genres if g and g not in (provider, "Sports")]
                    if cands:
                        sport_label = max(cands, key=len)
            except Exception:
                pass
        if sport_label and provider:
            desc_text = f"{base_desc} - {sport_label} - on {provider}"
        elif provider:
            desc_text = f"{base_desc} - on {provider}"
        else:
            desc_text = base_desc
        ET.SubElement(prog, "desc").text = desc_text

        ET.SubElement(prog, "category").text = provider
        ET.SubElement(prog, "category").text = "Sports"
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

    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    Path(xml_path).parent.mkdir(parents=True, exist_ok=True)
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote Direct XMLTV: {xml_path}")


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
    enabled_services = preferences.get("enabled_services", [])
    priority_map = preferences.get("service_priorities", {})
    amazon_penalty = preferences.get("amazon_penalty", True)
    language_preference = preferences.get("language_preference", "en")

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
        for idx, event in enumerate(events, start=1):
            pvid = event.get("pvid")
            if not pvid:
                continue

            chan_id = stable_channel_id(event, epg_prefix)
            title = event.get("title") or f"Sports Event {idx}"
            channel_name = event.get("channel_name") or "Sports"
            provider = get_provider_from_channel(channel_name)

            img_url = get_event_image_url(conn, event)

            event_id = event.get("id", "")
            deeplink_url = None
            reason = None

            try:
                cur.execute(
                    """SELECT provider, playable_url, deeplink_play, deeplink_open, priority, espn_graph_id
                           FROM playables
                           WHERE event_id = ?""",
                    (event_id,),
                )
                p_rows = cur.fetchall()
            except Exception:
                p_rows = []

            if FILTERING_AVAILABLE and p_rows:
                deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services, priority_map, amazon_penalty, language_preference)
                
                # ESPN FIX: Apply ESPN Graph ID correction to filtered result
                # get_best_deeplink_for_event returns the raw deeplink_play from database
                # We need to correct ESPN deeplinks to use Graph IDs when available
                if deeplink_url and deeplink_url.startswith("sportscenter://"):
                    try:
                        # Find ANY ESPN playable with a Graph ID (prefer enriched over un-enriched)
                        for prow in p_rows:
                            raw_provider = prow["provider"] if prow["provider"] else ""
                            if raw_provider.lower() not in ('sportscenter', 'espn', 'espn+'):
                                continue
                            
                            # sqlite3.Row dict-style access
                            espn_graph_id = prow["espn_graph_id"] if "espn_graph_id" in prow.keys() else None
                            if espn_graph_id:
                                parts = espn_graph_id.split(':')
                                if len(parts) >= 2:
                                    play_id = parts[1]
                                    deeplink_url = f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"
                                    break  # Use first ESPN playable with Graph ID
                    except Exception:
                        pass  # Fall back to original deeplink

            # Second pass: use logical_service_mapper directly on playables
            if (
                not deeplink_url
                and p_rows
                and FILTERING_AVAILABLE
                and enabled_services
            ):
                try:
                    from logical_service_mapper import get_logical_service_for_playable

                    best = None
                    for prow in p_rows:
                        raw_provider = (prow["provider"] or "").strip()
                        playable_url = (prow["playable_url"] or "").strip()
                        deeplink_play = (prow["deeplink_play"] or "").strip()
                        deeplink_open = (prow["deeplink_open"] or "").strip()
                        espn_graph_id = (prow.get("espn_graph_id") or "").strip() if "espn_graph_id" in prow.keys() else ""

                        url = deeplink_play or deeplink_open or playable_url
                        if not url:
                            continue

                        # ESPN FIX: Use ESPN Graph ID to generate working deeplinks
                        # Apple TV provides broken playChannel or wrong playID deeplinks
                        # ESPN Watch Graph provides correct playID deeplinks that work
                        if espn_graph_id and raw_provider.lower() in ('sportscenter', 'espn', 'espn+'):
                            try:
                                # Extract playID from ESPN Graph ID (format: espn-watch:{playID}:{hash})
                                parts = espn_graph_id.split(':')
                                if len(parts) >= 2:
                                    play_id = parts[1]
                                    # Generate corrected scheme deeplink
                                    url = f"sportscenter://x-callback-url/showWatchStream?playID={play_id}"
                                    deeplink_play = url
                            except Exception:
                                pass  # Fall back to original deeplink

                        logical_service = get_logical_service_for_playable(
                            provider=raw_provider,
                            deeplink_play=deeplink_play or None,
                            deeplink_open=deeplink_open or None,
                            playable_url=playable_url or None,
                            event_id=event_id,
                            conn=conn,
                        )
                        if not logical_service or logical_service not in enabled_services:
                            continue

                        prio = 0
                        try:
                            if "priority" in prow.keys() and prow["priority"] is not None:
                                prio = int(prow["priority"])
                        except Exception:
                            pass

                        if best is None or prio > best["priority"]:
                            best = {"url": url, "priority": prio}

                    if best is not None:
                        deeplink_url = best["url"]
                        reason = None
                except Exception:
                    # Don't break the whole export if mapping fails
                    pass


            if not deeplink_url:
                has_playables = bool(p_rows)
                has_raw_url = False

                raw = event.get("raw_attributes_json")
                if raw:
                    try:
                        data = json.loads(raw)
                        candidate = data.get("webUrl") or data.get("web_url") or data.get("url")
                        if isinstance(candidate, str) and candidate.startswith("http"):
                            has_raw_url = True
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

                if FILTERING_AVAILABLE:
                    raw_url_fallback = get_fallback_deeplink(event)
                else:
                    raw_url_fallback = None

                if raw_url_fallback:
                    deeplink_url = raw_url_fallback
                    reason = None

            if not deeplink_url:
                if reason is None:
                    reason = "no_url_for_any_service"
                bump(reason)
                skipped_no_deeplink += 1
                continue

            actual_provider = provider
            try:
                if FILTERING_AVAILABLE:
                    from logical_service_mapper import (
                        get_logical_service_for_playable,
                        get_service_display_name,
                    )
                    from provider_utils import extract_provider_from_url

                    scheme = extract_provider_from_url(deeplink_url)
                    if scheme:
                        logical_service = get_logical_service_for_playable(
                            provider=scheme if scheme not in ("http", "https") else scheme,
                            deeplink_play=deeplink_url,
                            deeplink_open=None,
                            playable_url=None,
                            event_id=event_id,
                            conn=conn,
                        )
                        actual_provider = get_service_display_name(logical_service)
            except Exception:
                actual_provider = provider

            if reason:
                bump(reason)

            logo_part = f'tvg-logo="{img_url}" ' if img_url else ""
            group_title = actual_provider or provider or "Sports"
            f.write(
                '#EXTINF:-1 tvg-id="{id}" tvg-name="{name}" {logo}group-title="{group}",{name}\n'.format(
                    id=chan_id,
                    name=title.replace(",", " "),
                    logo=logo_part,
                    group=group_title.replace('"', "'"),
                )
            )
            f.write(f"{deeplink_url}\n\n")

    print(f"Wrote Direct M3U: {m3u_path}")
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

