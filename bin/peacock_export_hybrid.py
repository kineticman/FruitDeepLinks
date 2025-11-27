#!/usr/bin/env python3
"""
peacock_export_hybrid.py - Export both lane-based AND direct deeplink formats

Generates:
1. Lane-based (for ADBTuner):
   - peacock_lanes.xml / peacock_lanes.m3u
   - M3U uses configurable server URLs that call /api/lane/{id}/deeplink
   
2. Direct deeplinks (for simple players):
   - peacock_direct.xml / peacock_direct.m3u
   - One channel per event (only events within 24 hours)
   - Includes placeholders: "Event Not Started" and "Event Ended"
   - Channels match between XML and M3U
"""

import os, argparse, json, sqlite3, urllib.parse
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

def get_lanes(conn: sqlite3.Connection) -> List[Tuple[int, str, int]]:
    cur = conn.cursor()
    cur.execute("SELECT lane_id, name, logical_number FROM lanes ORDER BY lane_id")
    return [(row["lane_id"], row["name"], row["logical_number"]) for row in cur.fetchall()]

def get_lane_events(conn: sqlite3.Connection) -> List[Dict]:
    cur = conn.cursor()
    cur.execute("""
        SELECT le.lane_id, le.event_id, le.is_placeholder, le.start_utc, le.end_utc, le.title,
               e.pvid, e.slug, e.title AS event_title, e.channel_name,
               e.synopsis, e.synopsis_brief, e.genres_json
        FROM lane_events le
        LEFT JOIN events e ON le.event_id = e.id
        ORDER BY le.lane_id, le.start_utc
    """)
    return [dict(row) for row in cur.fetchall()]

def get_direct_events(conn: sqlite3.Connection, hours_window: int = 24) -> List[Dict]:
    """Get events starting within the next X hours"""
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(hours=hours_window)
    
    cur.execute("""
        SELECT e.id, e.pvid, e.slug, e.title, e.channel_name,
               e.synopsis, e.synopsis_brief, e.genres_json,
               le.start_utc, le.end_utc
        FROM events e
        JOIN lane_events le ON e.id = le.event_id
        WHERE le.is_placeholder = 0
          AND e.pvid IS NOT NULL
          AND le.start_utc <= ?
          AND le.end_utc > ?
        GROUP BY e.id
        ORDER BY le.start_utc
    """, (window_end.isoformat(), now.isoformat()))
    
    return [dict(row) for row in cur.fetchall()]

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime.max.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def snap_to_half_hour(dt: datetime) -> datetime:
    """Snap datetime to nearest :00 or :30"""
    if dt.minute < 15:
        return dt.replace(minute=0, second=0, microsecond=0)
    elif dt.minute < 45:
        return dt.replace(minute=30, second=0, microsecond=0)
    else:
        # Round up to next hour
        return (dt + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

def xmltv_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S +0000")

def format_local_time(dt: datetime) -> str:
    """Format datetime in local time for display"""
    # Convert to US Eastern Time (you can change this to your timezone)
    from datetime import timezone as tz
    # EST is UTC-5, EDT is UTC-4
    # For simplicity, using a fixed offset. In production, use pytz or zoneinfo
    eastern_offset = timedelta(hours=-5)  # Adjust for your timezone
    local_dt = dt + eastern_offset
    return local_dt.strftime('%I:%M %p EST')

def get_event_images(conn: sqlite3.Connection, event_id: str, preferred_types: List[str]) -> Optional[str]:
    if not event_id:
        return None
    cur = conn.cursor()
    for img_type in preferred_types:
        cur.execute("SELECT url FROM event_images WHERE event_id=? AND img_type=? LIMIT 1", (event_id, img_type))
        row = cur.fetchone()
        if row:
            return row["url"]
    cur.execute("SELECT url FROM event_images WHERE event_id=? LIMIT 1", (event_id,))
    row = cur.fetchone()
    return row["url"] if row else None

def build_adbtuner_xmltv(conn: sqlite3.Connection, xml_path: str):
    """Build lane-based XMLTV for ADBTuner"""
    lanes = get_lanes(conn)
    lane_events = get_lane_events(conn)
    print(f"ADBTuner XMLTV: {len(lanes)} lanes, {len(lane_events)} events")
    
    events_by_lane: Dict[int, List[Dict]] = {}
    for row in lane_events:
        events_by_lane.setdefault(row["lane_id"], []).append(row)
    
    tv = ET.Element("tv")
    tv.set("generator-info-name", "Peacock TV Scraper")
    tv.set("generator-info-url", "https://github.com/kineticman/PeacockDeepLinks")
    
    # Channels
    for lane_id, name, logical_number in lanes:
        chan = ET.SubElement(tv, "channel", id=f"peacock.lane.{lane_id}")
        dn = ET.SubElement(chan, "display-name")
        dn.text = f"{name} ({logical_number})"
    
    # Programs
    for lane_id, name, logical_number in lanes:
        rows = events_by_lane.get(lane_id, [])
        if not rows:
            continue
        
        for row in rows:
            start = parse_iso(row["start_utc"])
            stop = parse_iso(row["end_utc"])
            if stop <= start:
                stop = start + timedelta(minutes=1)
            
            prog = ET.SubElement(
                tv, "programme",
                channel=f"peacock.lane.{lane_id}",
                start=xmltv_time(start),
                stop=xmltv_time(stop)
            )
            
            is_placeholder = bool(row["is_placeholder"])
            
            # Title
            if is_placeholder:
                title_text = "Nothing Scheduled"
            else:
                title_text = row.get("event_title") or row.get("title") or "Peacock Sports"
            
            title_el = ET.SubElement(prog, "title")
            title_el.text = title_text
            
            # Description
            if not is_placeholder:
                desc_text = row.get("synopsis") or row.get("synopsis_brief")
                if desc_text:
                    desc_el = ET.SubElement(prog, "desc")
                    desc_el.text = desc_text
            
            # Categories
            if not is_placeholder:
                cat1 = ET.SubElement(prog, "category")
                cat1.text = "Sports"
                
                genres_json = row.get("genres_json")
                if genres_json:
                    try:
                        genres = json.loads(genres_json)
                        if isinstance(genres, list):
                            for g in genres:
                                if g:
                                    cat_el = ET.SubElement(prog, "category")
                                    cat_el.text = str(g)
                    except:
                        pass
            
            # Icon
            if not is_placeholder and row.get("event_id"):
                img_url = get_event_images(
                    conn, row["event_id"],
                    ["landscape", "scene169", "titleArt169", "scene34"]
                )
                if img_url:
                    icon = ET.SubElement(prog, "icon")
                    icon.set("src", img_url)
            
            if not is_placeholder:
                live = ET.SubElement(prog, "live")
                live.text = "1"
    
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote ADBTuner XMLTV: {xml_path}")

def build_adbtuner_m3u(conn: sqlite3.Connection, m3u_path: str, server_url: str):
    """Build lane-based M3U for ADBTuner with API URLs"""
    lanes = get_lanes(conn)
    print(f"ADBTuner M3U: {len(lanes)} lanes")
    
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        for lane_id, name, logical_number in lanes:
            # Use configured server URL for API endpoint
            stream_url = f"{server_url}/api/lane/{lane_id}/deeplink"
            
            f.write(
                f'#EXTINF:-1 tvg-id="peacock.lane.{lane_id}" '
                f'tvg-name="{name}" '
                f'tvg-chno="{logical_number}" '
                f'group-title="Peacock Lanes" tvg-logo="",{name}\n'
            )
            f.write(f"{stream_url}\n\n")
    
    print(f"Wrote ADBTuner M3U: {m3u_path}")

def build_chrome_m3u(conn: sqlite3.Connection, m3u_path: str, server_url: str):
    """Build Chrome Capture M3U with chrome:// API URLs for dynamic deeplinks"""
    lanes = get_lanes(conn)
    print(f"Chrome Capture M3U: {len(lanes)} lanes")
    
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        for lane_id, name, logical_number in lanes:
            # Use API endpoint wrapped in chrome:// for dynamic deeplink resolution
            api_url = f"{server_url}/api/lane/{lane_id}/deeplink?format=text"
            chrome_url = f"chrome://{api_url}"
            
            f.write(
                f'#EXTINF:-1 tvg-id="peacock.lane.{lane_id}" '
                f'tvg-name="{name}" '
                f'tvg-chno="{logical_number}" '
                f'group-title="Peacock Lanes" tvg-logo="",{name}\n'
            )
            f.write(f"{chrome_url}\n\n")
    
    print(f"Wrote Chrome Capture M3U: {m3u_path}")

def build_direct_xmltv(conn: sqlite3.Connection, xml_path: str):
    """Build one-channel-per-event XMLTV with placeholders"""
    events = get_direct_events(conn, hours_window=24)
    print(f"Direct XMLTV: {len(events)} event channels (within 24 hours)")
    
    now = datetime.now(timezone.utc)
    
    tv = ET.Element("tv")
    tv.set("generator-info-name", "Peacock TV Scraper - Direct")
    tv.set("generator-info-url", "https://github.com/kineticman/PeacockDeepLinks")
    
    # Create channel and program for each event with placeholders
    for idx, event in enumerate(events, start=1):
        chan_id = f"peacock.event.{idx}"
        
        # Channel definition
        chan = ET.SubElement(tv, "channel", id=chan_id)
        dn = ET.SubElement(chan, "display-name")
        dn.text = event["title"] or f"Peacock Event {idx}"
        
        # Event times
        event_start = parse_iso(event["start_utc"])
        event_end = parse_iso(event["end_utc"])
        if event_end <= event_start:
            event_end = event_start + timedelta(hours=3)
        
        # Placeholder: "Event Not Started" - snap to :00 or :30
        # Start from NOW or 8 hours before event (whichever is earlier)
        pre_start = now
        earliest_start = event_start - timedelta(hours=8)
        if earliest_start < pre_start:
            pre_start = earliest_start
        
        # Snap to nearest :00 or :30
        pre_start = snap_to_half_hour(pre_start)
        
        # Create 30-minute placeholder blocks before event
        current = pre_start
        while current < event_start:
            block_end = min(current + timedelta(minutes=30), event_start)
            
            # Skip if block would be less than 1 minute
            if (block_end - current).total_seconds() < 60:
                break
            
            prog = ET.SubElement(
                tv, "programme",
                channel=chan_id,
                start=xmltv_time(current),
                stop=xmltv_time(block_end)
            )
            
            title_el = ET.SubElement(prog, "title")
            title_el.text = "Event Not Started"
            
            desc_el = ET.SubElement(prog, "desc")
            desc_el.text = f"This event starts at {format_local_time(event_start)}. Check back closer to start time."
            
            current = block_end
        
        # Actual event program
        prog = ET.SubElement(
            tv, "programme",
            channel=chan_id,
            start=xmltv_time(event_start),
            stop=xmltv_time(event_end)
        )
        
        title_el = ET.SubElement(prog, "title")
        title_el.text = event["title"] or "Peacock Sports"
        
        desc_text = event.get("synopsis") or event.get("synopsis_brief")
        if desc_text:
            desc_el = ET.SubElement(prog, "desc")
            desc_el.text = desc_text
        
        cat1 = ET.SubElement(prog, "category")
        cat1.text = "Sports"
        
        genres_json = event.get("genres_json")
        if genres_json:
            try:
                genres = json.loads(genres_json)
                if isinstance(genres, list):
                    for g in genres:
                        if g:
                            cat_el = ET.SubElement(prog, "category")
                            cat_el.text = str(g)
            except:
                pass
        
        if event.get("id"):
            img_url = get_event_images(
                conn, event["id"],
                ["landscape", "scene169", "titleArt169", "scene34"]
            )
            if img_url:
                icon = ET.SubElement(prog, "icon")
                icon.set("src", img_url)
        
        live = ET.SubElement(prog, "live")
        live.text = "1"
        
        # Placeholder: "Event Ended" (24 hours after event end)
        post_end = event_end + timedelta(hours=24)
        
        # Snap event_end to next :00 or :30
        current = snap_to_half_hour(event_end)
        if current < event_end:
            current = event_end
        
        # Create 30-minute placeholder blocks after event
        while current < post_end:
            block_end = min(current + timedelta(minutes=30), post_end)
            
            # Skip if block would be less than 1 minute
            if (block_end - current).total_seconds() < 60:
                break
            
            prog = ET.SubElement(
                tv, "programme",
                channel=chan_id,
                start=xmltv_time(current),
                stop=xmltv_time(block_end)
            )
            
            title_el = ET.SubElement(prog, "title")
            title_el.text = "Event Ended"
            
            desc_el = ET.SubElement(prog, "desc")
            desc_el.text = f"This event ended at {format_local_time(event_end)}. Check guide for upcoming events."
            
            current = block_end
    
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote Direct XMLTV: {xml_path}")

def build_direct_m3u(conn: sqlite3.Connection, m3u_path: str):
    """Build one-channel-per-event M3U matching the XMLTV channels"""
    events = get_direct_events(conn, hours_window=24)
    print(f"Direct M3U: {len(events)} event channels (within 24 hours)")
    
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        
        for idx, event in enumerate(events, start=1):
            pvid = event.get("pvid")
            if not pvid:
                continue
            
            # Create actual Peacock deeplink
            deeplink_payload = {"pvid": pvid, "type": "PROGRAMME", "action": "PLAY"}
            deeplink_json = json.dumps(deeplink_payload, separators=(",", ":"))
            deeplink_url = f"https://www.peacocktv.com/deeplink?deeplinkData={urllib.parse.quote(deeplink_json, safe='')}"
            
            # MUST match XMLTV channel ID
            chan_id = f"peacock.event.{idx}"
            title = event["title"] or f"Peacock Event {idx}"
            
            # Get image for tvg-logo
            logo_url = ""
            if event.get("id"):
                logo_url = get_event_images(
                    conn, event["id"],
                    ["landscape", "scene169", "titleArt169", "scene34"]
                ) or ""
            
            f.write(
                f'#EXTINF:-1 tvg-id="{chan_id}" '
                f'tvg-name="{title}" '
                f'group-title="Peacock Events"'
            )
            
            if logo_url:
                f.write(f' tvg-logo="{logo_url}"')
            
            f.write(f',{title}\n')
            f.write(f"{deeplink_url}\n\n")
    
    print(f"Wrote Direct M3U: {m3u_path}")

def main():
    script_dir = Path(__file__).resolve().parent
    if script_dir.name == 'bin':
        repo_root = script_dir.parent
        default_db = str(repo_root / 'data' / 'peacock_events.db')
        default_lanes_xml = str(repo_root / 'out' / 'peacock_lanes.xml')
        default_lanes_m3u = str(repo_root / 'out' / 'peacock_lanes.m3u')
        default_direct_xml = str(repo_root / 'out' / 'peacock_direct.xml')
        default_direct_m3u = str(repo_root / 'out' / 'peacock_direct.m3u')
    else:
        default_db = "peacock_events.db"
        default_lanes_xml = "peacock_lanes.xml"
        default_lanes_m3u = "peacock_lanes.m3u"
        default_direct_xml = "peacock_direct.xml"
        default_direct_m3u = "peacock_direct.m3u"
    
    env_db = os.getenv("PEACOCK_DB_PATH")
    env_lanes_xml = os.getenv("PEACOCK_LANES_XML_PATH")
    env_lanes_m3u = os.getenv("PEACOCK_LANES_M3U_PATH")
    env_direct_xml = os.getenv("PEACOCK_DIRECT_XML_PATH")
    env_direct_m3u = os.getenv("PEACOCK_DIRECT_M3U_PATH")
    env_server_url = os.getenv("PEACOCK_SERVER_URL")
    env_chrome_m3u = os.getenv("PEACOCK_CHROME_M3U_PATH")
    
    # Get server URL from HOST+PORT if not set
    if not env_server_url:
        server_host = os.getenv("PEACOCK_SERVER_HOST", "localhost")
        server_port = os.getenv("PEACOCK_PORT", "6655")
        env_server_url = f"http://{server_host}:{server_port}"
    
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=env_db or default_db)
    ap.add_argument("--lanes-xml", default=env_lanes_xml or default_lanes_xml)
    ap.add_argument("--lanes-m3u", default=env_lanes_m3u or default_lanes_m3u)
    ap.add_argument("--chrome-m3u", default=env_chrome_m3u or default_lanes_m3u.replace('.m3u', '_chrome.m3u'))
    ap.add_argument("--direct-xml", default=env_direct_xml or default_direct_xml)
    ap.add_argument("--direct-m3u", default=env_direct_m3u or default_direct_m3u)
    ap.add_argument("--server-url", default=env_server_url, help="Server URL for API deeplink endpoints")
    args = ap.parse_args()
    
    for path in [args.lanes_xml, args.lanes_m3u, args.chrome_m3u, args.direct_xml, args.direct_m3u]:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Using DB: {args.db}")
    print(f"Server URL: {args.server_url}")
    print(f"\nADBTuner outputs:")
    print(f"  - {args.lanes_xml}")
    print(f"  - {args.lanes_m3u}")
    print(f"\nChrome Capture output:")
    print(f"  - {args.chrome_m3u}")
    print(f"\nDirect outputs:")
    print(f"  - {args.direct_xml}")
    print(f"  - {args.direct_m3u}")
    print()
    
    conn = get_conn(args.db)
    ok, missing = check_tables(conn, ["lanes", "lane_events", "events"])
    if not ok:
        print(f"\nERROR: Missing tables: {', '.join(missing)}")
        print("\nRun: ./bin/peacock_refresh_all.py")
        return 1
    
    # Build ADBTuner files (lane-based with API URLs)
    build_adbtuner_xmltv(conn, args.lanes_xml)
    build_adbtuner_m3u(conn, args.lanes_m3u, args.server_url)
    build_chrome_m3u(conn, args.chrome_m3u, args.server_url)
    
    # Build Direct files (one channel per event with deeplinks)
    build_direct_xmltv(conn, args.direct_xml)
    build_direct_m3u(conn, args.direct_m3u)
    
    conn.close()
    print("\nExport complete!")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
