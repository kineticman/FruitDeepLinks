# bin/peacock_export_hybrid.py
#!/usr/bin/env python3
"""
Export XMLTV + M3U with STABLE channel ids that match M3U tvg-id.

Updates:
- Placeholder times (upcoming/ended) are rendered in **system local time** with tz abbrev.
- Stable XML <channel id> == M3U tvg-id via fdl.<event_id|pvid>
- Deterministic SQL ordering
- 24h default window, placeholders, provider categories, image extraction, deeplinks
"""

from __future__ import annotations

import os, argparse, json, sqlite3, urllib.parse, sys, re
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
        get_fallback_deeplink
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

# -------------------- DB helpers --------------------
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def check_tables(conn: sqlite3.Connection, required: List[str]) -> Tuple[bool, List[str]]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row["name"] for row in cur.fetchall()}
    missing = [t for t in required if t not in existing]
    return (len(missing) == 0, missing)

# -------------------- Small utils --------------------
def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime.max.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def xmltv_time(dt: datetime) -> str:
    # Keep the space form for broad compatibility; can switch to no-space if needed.
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S +0000")

def snap_to_half_hour(dt: datetime) -> datetime:
    if dt.minute < 15:
        return dt.replace(minute=0, second=0, microsecond=0)
    elif dt.minute < 45:
        return dt.replace(minute=30, second=0, microsecond=0)
    else:
        return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

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
    st = (event.get("start_utc") or "").replace("-", "").replace(":", "").replace("T", "").replace("Z", "")
    return _sanitize_id(prefix + t + "." + st)

def get_provider_from_channel(channel_name: str) -> str:
    if not channel_name:
        return "Sports"
    channel_lower = channel_name.lower()
    if "espn" in channel_lower:
        return "ESPN+"
    elif "peacock" in channel_lower:
        return "Peacock"
    elif "national broadcasting company" in channel_lower or channel_name == "National Broadcasting Company":
        return "Peacock"
    elif "nbc sports" in channel_lower:
        return "NBC Sports"
    elif "prime" in channel_lower or "amazon" in channel_lower:
        return "Prime Video"
    elif "cbs" in channel_lower:
        return "CBS Sports"
    elif "paramount" in channel_lower:
        return "Paramount+"
    elif "fox" in channel_lower:
        return "FOX Sports"
    elif "nfl" in channel_lower and "network" not in channel_lower:
        return "NFL+"
    elif "nba" in channel_lower and "tv" not in channel_lower:
        return "NBA League Pass"
    elif "dazn" in channel_lower:
        return "DAZN"
    elif "apple" in channel_lower:
        return "Apple TV+"
    else:
        return channel_name

# Local time display helpers
_LOCAL_TZ = datetime.now().astimezone().tzinfo

def _fmt_local_short(dt_utc: datetime) -> str:
    """
    WHY: Human-friendly local time for placeholders. Example: 'Sun at 04:00 AM EST'
    Notes:
      - Portable: avoid %-I (Linux) / %#I (Windows). Use %I and accept leading zero.
      - %Z gets tz abbrev (EST/EDT/etc.). Fallback to offset if empty.
    """
    ldt = dt_utc.astimezone(_LOCAL_TZ)
    tz = ldt.strftime("%Z") or ldt.strftime("%z")
    return ldt.strftime(f"%a at %I:%M %p {tz}")

# -------------------- Images --------------------
def get_event_image_url(conn: sqlite3.Connection, event: Dict) -> Optional[str]:
    event_id = event.get("id") or event.get("event_id")
    if event_id:
        cur = conn.cursor()
        for img_type in ["landscape", "scene169", "titleArt169"]:
            cur.execute("SELECT url FROM event_images WHERE event_id=? AND img_type=? LIMIT 1",
                        (event_id, img_type))
            row = cur.fetchone()
            if row:
                return row["url"]

    raw_json = event.get("raw_attributes_json")
    if raw_json:
        try:
            attrs = json.loads(raw_json)
            if "competitors" in attrs and isinstance(attrs["competitors"], list):
                for comp in attrs["competitors"]:
                    logo_url = comp.get("logo_url")
                    if logo_url:
                        return (logo_url
                                .replace("{w}", "400")
                                .replace("{h}", "400")
                                .replace("{f}", "png"))
            if "images" in attrs and attrs["images"]:
                images = attrs["images"]
                for key in ["showTile2x1", "showTile16x9", "showTile2x3"]:
                    if key in images and images[key]:
                        return images[key]
            if "playables" in attrs and isinstance(attrs["playables"], list):
                for playable in attrs["playables"]:
                    if playable.get("image"):
                        return playable["image"]
        except Exception:
            pass
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
               e.start_utc, e.end_utc, e.raw_attributes_json
        FROM events e
        WHERE e.pvid IS NOT NULL
          AND e.end_utc >= ?
          AND e.start_utc <= ?
        ORDER BY datetime(e.start_utc) ASC,
                 datetime(e.end_utc) ASC,
                 e.title ASC,
                 e.id ASC
        """,
        (now.isoformat(), window_end.isoformat()),
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
            enabled_services = preferences.get("enabled_services", [])
            disabled_sports = preferences.get("disabled_sports", [])
            disabled_leagues = preferences.get("disabled_leagues", [])

            # Compute disabled services relative to all logical services seen in DB
            disabled_services: List[str] = []
            try:
                from logical_service_mapper import get_all_logical_services_with_counts

                service_counts = get_all_logical_services_with_counts(conn)
                all_services = sorted(service_counts.keys())
                if enabled_services:
                    disabled_services = [s for s in all_services if s not in enabled_services]
                else:
                    # enabled_services == [] means ALL are allowed
                    disabled_services = []
            except Exception:
                # If logical_service_mapper is unavailable, skip disabled-services breakdown
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
                "    Disabled sports ({count}): {items}".format(
                    count=len(disabled_sports),
                    items=", ".join(sorted(disabled_sports)) if disabled_sports else "None",
                )
            )
            print(
                "    Disabled leagues ({count}): {items}".format(
                    count=len(disabled_leagues),
                    items=", ".join(sorted(disabled_leagues)) if disabled_leagues else "None",
                )
            )
            removed = len(all_events) - len(filtered_events)
            print(
                "    Events kept: {kept} / {total} (removed {removed})".format(
                    kept=len(filtered_events),
                    total=len(all_events),
                    removed=removed,
                )
            )

        return filtered_events

    # If filters were requested but filter_integration isn't available, say so once
    if log_summary and apply_filters and not FILTERING_AVAILABLE:
        print("  Filter settings: filtering requested but filter_integration is not available")

    return all_events
def build_direct_xmltv(conn: sqlite3.Connection, xml_path: str, hours_window: int = 24, 
                       epg_prefix: str = "fdl.", apply_filters: bool = True):
    events = get_direct_events(conn, hours_window=hours_window, apply_filters=apply_filters, log_summary=True)
    print(f"Direct XMLTV: {len(events)} event channels (within {hours_window}h)")
    
    # Load user preferences for deeplink selection
    preferences = load_user_preferences(conn) if FILTERING_AVAILABLE else {}
    enabled_services = preferences.get("enabled_services", [])

    now = datetime.now(timezone.utc)
    tv = ET.Element("tv")
    tv.set("generator-info-name", "FruitDeepLinks - Direct")
    tv.set("generator-info-url", "https://github.com/yourusername/FruitDeepLinks")

    for idx, event in enumerate(events, start=1):
        chan_id = stable_channel_id(event, epg_prefix)
        title = event.get("title") or f"Sports Event {idx}"
        channel_name = event.get("channel_name") or "Sports"
        event_id = event.get("id", "")
        
        # Get actual deeplink and extract provider from it (same as M3U)
        deeplink_url = None
        if FILTERING_AVAILABLE:
            # Try filtered playables first
            deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services)
        
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
        
        # Extract actual provider from the deeplink URL
        provider = "Sports"  # Default fallback
        if deeplink_url:
            try:
                # Try using logical service mapper first
                if FILTERING_AVAILABLE:
                    from logical_service_mapper import get_logical_service_for_playable, get_service_display_name
                    from provider_utils import extract_provider_from_url
                    
                    # Extract raw provider scheme
                    scheme = extract_provider_from_url(deeplink_url)
                    if scheme:
                        # Get logical service (handles web URL mapping)
                        logical_service = get_logical_service_for_playable(
                            provider=scheme if scheme not in ('http', 'https') else scheme,
                            deeplink_play=deeplink_url,
                            deeplink_open=None,
                            playable_url=None,
                            event_id=event_id,
                            conn=conn
                        )
                        provider = get_service_display_name(logical_service)
            except Exception as e:
                # Fallback to channel_name if all else fails
                provider = get_provider_from_channel(channel_name)

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
            pre_prog = ET.SubElement(tv, "programme",
                                     channel=chan_id,
                                     start=xmltv_time(current),
                                     stop=xmltv_time(block_end))
            ET.SubElement(pre_prog, "title").text = "Event Not Started"
            # WHY: show *local* start time for user clarity
            ET.SubElement(pre_prog, "desc").text = f"Starts { _fmt_local_short(event_start) }. Available on {provider}."
            current = block_end

        # Main event
        prog = ET.SubElement(tv, "programme",
                             channel=chan_id,
                             start=xmltv_time(event_start),
                             stop=xmltv_time(event_end))
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
                    if g and g != provider:
                        ET.SubElement(prog, "category").text = str(g)
            except Exception:
                pass

        img_url = get_event_image_url(conn, event)
        if img_url:
            ET.SubElement(prog, "icon", src=img_url)
        ET.SubElement(prog, "live").text = "1"

        # Post-event placeholders (24h in 1h blocks)
        current = event_end
        post_end = event_end + timedelta(hours=24)
        while current < post_end:
            block_end = min(current + timedelta(hours=1), post_end)
            post_prog = ET.SubElement(tv, "programme",
                                      channel=chan_id,
                                      start=xmltv_time(current),
                                      stop=xmltv_time(block_end))
            ET.SubElement(post_prog, "title").text = "Event Ended"
            # WHY: show *local* end time too
            ET.SubElement(post_prog, "desc").text = f"Ended { _fmt_local_short(event_end) }. Available on {provider}."
            current = block_end

    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    Path(xml_path).parent.mkdir(parents=True, exist_ok=True)
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote Direct XMLTV: {xml_path}")

# -------------------- M3U --------------------
def build_direct_m3u(conn: sqlite3.Connection, m3u_path: str, hours_window: int = 24, 
                    epg_prefix: str = "fdl.", apply_filters: bool = True):
    events = get_direct_events(conn, hours_window=hours_window, apply_filters=apply_filters)
    print(f"Direct M3U: {len(events)} event channels (within {hours_window}h)")
    
    # Load user preferences for deeplink selection
    preferences = load_user_preferences(conn) if FILTERING_AVAILABLE else {}
    enabled_services = preferences.get("enabled_services", [])
    
    skipped_no_deeplink = 0
    reason_counts: Dict[str, int] = {}
    service_skip_counts: Dict[str, int] = {}

    def bump(reason: str) -> None:
        reason_counts[reason] = reason_counts.get(reason, 0) + 1

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
            logo_attr = f' tvg-logo="{img_url}"' if img_url else ""

            # NEW: Smart deeplink selection based on user preferences
            deeplink_url = None
            event_id = event.get("id", "")
            
            if FILTERING_AVAILABLE:
                # Try filtered playables first
                deeplink_url = get_best_deeplink_for_event(conn, event_id, enabled_services)
            
            if not deeplink_url and FILTERING_AVAILABLE:
                # Fallback to raw_attributes (for events without playables table data)
                deeplink_url = get_fallback_deeplink(event)
            
            if not deeplink_url:
                # Final fallback: old method for Peacock events
                if not event_id.startswith("appletv-"):
                    payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
                    deeplink_url = "https://www.peacocktv.com/deeplink?deeplinkData=" + urllib.parse.quote(
                        json.dumps(payload, separators=(",", ":"), ensure_ascii=False), safe=""
                    )
            
            if not deeplink_url:
                # NEW: Apple TV fallback - use playable_url from playables table
                cur.execute('''
                    SELECT playable_url 
                    FROM playables 
                    WHERE event_id = ? AND playable_url IS NOT NULL 
                    ORDER BY priority ASC
                    LIMIT 1
                ''', (event_id,))
                row = cur.fetchone()
                if row:
                    deeplink_url = row[0]
            
            # Skip events with no suitable deeplink
            if not deeplink_url:
                # Classify why this event ended up without a deeplink
                reason = "unknown"

                raw_attrs = event.get("raw_attributes_json") or ""
                has_raw_url = ("http://" in raw_attrs) or ("https://" in raw_attrs) or ("videos://" in raw_attrs)

                # Inspect playables to see if this is service-filter related
                try:
                    cur.execute(
                        """SELECT provider, playable_url, deeplink_play, deeplink_open
                               FROM playables
                               WHERE event_id = ?""",
                        (event_id,),
                    )
                    p_rows = cur.fetchall()
                except Exception:
                    p_rows = []

                logical_services = set()
                if p_rows:
                    try:
                        from logical_service_mapper import get_logical_service_for_playable
                    except Exception:
                        get_logical_service_for_playable = None

                    if get_logical_service_for_playable is not None:
                        for r in p_rows:
                            try:
                                ls = get_logical_service_for_playable(
                                    provider=r["provider"],
                                    deeplink_play=r["deeplink_play"],
                                    deeplink_open=r["deeplink_open"],
                                    playable_url=r["playable_url"],
                                    event_id=event_id,
                                    conn=conn,
                                )
                                if ls:
                                    logical_services.add(ls)
                            except Exception:
                                continue

                if p_rows and not logical_services:
                    # We had playables but couldn't classify them
                    reason = "no_logical_service"
                elif p_rows and logical_services:
                    if enabled_services:
                        # All logical services for this event are currently DISABLED
                        if not any(ls in enabled_services for ls in logical_services):
                            reason = "filtered_by_services"
                            # Track which logical services were responsible for the skip
                            for ls in logical_services:
                                service_skip_counts[ls] = service_skip_counts.get(ls, 0) + 1
                        else:
                            # We have at least one allowed service but still ended up without a URL
                            reason = "no_url_for_allowed_services"
                    else:
                        # No enabled_services list means 'all services allowed'
                        reason = "no_url_for_any_service"
                elif not p_rows and has_raw_url:
                    reason = "raw_attributes_only"
                elif not p_rows and not has_raw_url:
                    reason = "no_playables_no_rawattrs"

                bump(reason)
                skipped_no_deeplink += 1
                continue
            
            # Extract actual provider from the deeplink URL for accurate group-title
            actual_provider = provider  # Default to channel_name-based provider
            try:
                if FILTERING_AVAILABLE:
                    from logical_service_mapper import get_logical_service_for_playable, get_service_display_name
                    from provider_utils import extract_provider_from_url
                    
                    # Extract raw provider scheme
                    scheme = extract_provider_from_url(deeplink_url)
                    if scheme:
                        # Get logical service (handles web URL mapping)
                        logical_service = get_logical_service_for_playable(
                            provider=scheme if scheme not in ('http', 'https') else scheme,
                            deeplink_play=deeplink_url,
                            deeplink_open=None,
                            playable_url=None,
                            event_id=event_id,
                            conn=conn
                        )
                        actual_provider = get_service_display_name(logical_service)
            except Exception as e:
                # Fallback to channel_name if all else fails
                actual_provider = get_provider_from_channel(channel_name)

            f.write(
                f'#EXTINF:-1 tvg-id="{chan_id}" tvg-name="{title}" group-title="{actual_provider}"{logo_attr},{title}\n'
            )
            f.write(f"{deeplink_url}\n\n")
    
    if skipped_no_deeplink > 0:
        print(f"  Skipped {skipped_no_deeplink} events with no suitable deeplinks")
        for reason, count in sorted(reason_counts.items(), key=lambda kv: -kv[1]):
            print(f"    - {reason}: {count}")
        if service_skip_counts:
            # Extra detail for filtered_by_services: which logical services were disabled
            breakdown = ", ".join(
                f"{svc}: {cnt}" for svc, cnt in sorted(service_skip_counts.items(), key=lambda kv: -kv[1])
            )
            print(f"      filtered_by_services breakdown: {breakdown}")

    Path(m3u_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"Wrote Direct M3U: {m3u_path}")

# -------------------- Stubs for lanes (unchanged) --------------------
def build_adbtuner_xmltv(conn, xml_path): print("Skipping lanes XMLTV - use full version")
def build_adbtuner_m3u(conn, m3u_path, server_url): print("Skipping lanes M3U - use full version")
def build_chrome_m3u(conn, m3u_path, server_url): print("Skipping chrome M3U - use full version")

# -------------------- CLI --------------------
def main():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent if script_dir.name == 'bin' else script_dir

    default_db = str(repo_root / 'data' / 'fruit_events.db')
    default_direct_xml = str(repo_root / 'out' / 'direct.xml')
    default_direct_m3u = str(repo_root / 'out' / 'direct.m3u')

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("PEACOCK_DB_PATH", default_db))
    ap.add_argument("--direct-xml", default=default_direct_xml)
    ap.add_argument("--direct-m3u", default=default_direct_m3u)
    ap.add_argument("--hours-window", type=int, default=24, help="Channelize events overlapping next N hours")
    ap.add_argument("--epg-prefix", default="fdl.", help="Prefix for tvg-id/<channel id>")
    ap.add_argument("--no-filters", action="store_true", help="Disable user preference filtering")
    args = ap.parse_args()
    
    apply_filters = not args.no_filters  # Default: apply filters unless --no-filters flag

    print(f"Using DB: {args.db}")
    print(f"Direct outputs: {args.direct_xml}, {args.direct_m3u}")
    print(f"Window: {args.hours_window}h | EPG prefix: {args.epg_prefix}")
    print(f"Filtering: {'ENABLED' if apply_filters else 'DISABLED'}")
    if apply_filters and not FILTERING_AVAILABLE:
        print("  Warning: filter_integration.py not found, filtering disabled")
    print()

    conn = get_conn(args.db)
    ok, missing = check_tables(conn, ["events"])
    if not ok:
        print(f"ERROR: Missing tables: {', '.join(missing)}")
        return 1

    build_direct_xmltv(conn, args.direct_xml, hours_window=args.hours_window, 
                      epg_prefix=args.epg_prefix, apply_filters=apply_filters)
    build_direct_m3u(conn, args.direct_m3u, hours_window=args.hours_window, 
                    epg_prefix=args.epg_prefix, apply_filters=apply_filters)

    conn.close()
    print("\nExport complete!")
    return 0

if __name__ == "__main__":
    sys.exit(main())