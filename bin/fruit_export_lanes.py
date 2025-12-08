#!/usr/bin/env python3
"""
fruit_export_lanes.py - Export FruitDeepLinks virtual channel lanes to XMLTV/M3U

Reads from lanes and lane_events tables to create scheduled programming
across virtual channels (like a TV network).
"""

import os, argparse, json, sqlite3, urllib.parse, sys
import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# -------------------- DB helpers --------------------
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def parse_iso(dt_str: str) -> datetime:
    if not dt_str:
        return datetime.max.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def xmltv_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S +0000")

def get_provider_from_channel(channel_name: str) -> str:
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
    else:
        return channel_name

# -------------------- Image helper (matches fruit_export_hybrid) --------------------
def get_event_image_url(conn: sqlite3.Connection, event: Dict) -> Optional[str]:
    """
    Resolve a best-guess image URL for a lane event.

    Priority:
      1. event_images table (if populated)
      2. Root Apple images, in this order:
         - shelfItemImagePost   (=> gen/1280x720Sports.TVAPoM02.jpg?... style)
         - shelfItemImage
         - shelfItemImageLive
         - shelfImageLogo
         - hero / scene169 / landscape / posterArt
      3. Competitor team logos (teamLogo*, scoreLogo, eventLogo) – SKIP masterArtLogo
      4. competitor.logo_url
      5. Playable-level images

    All templated URLs ({w}x{h}{c}.{f}) are normalized to 1280x720, jpg.
    """

    def _normalize_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        # Normalize all Apple/ESPN templates to 1280x720 jpg
        replacements = {
            "{w}": "1280",
            "{h}": "720",
            "{f}": "jpg",
            "{c}": "",          # strips the 'bb' / other format tokens
        }
        for token, repl in replacements.items():
            url = url.replace(token, repl)
        return url

    def _extract(val) -> Optional[str]:
        if isinstance(val, str):
            u = val
        elif isinstance(val, dict):
            u = val.get("url") or val.get("href")
        else:
            return None
        return _normalize_url(u)

    cur = conn.cursor()

    # 1) event_images table (legacy / ESPN pipelines)
    event_id = event.get("id") or event.get("event_id")
    if event_id:
        for img_type in ["landscape", "scene169", "titleArt169", "scene34"]:
            cur.execute(
                "SELECT url FROM event_images WHERE event_id=? AND img_type=? LIMIT 1",
                (event_id, img_type),
            )
            row = cur.fetchone()
            if row and row["url"]:
                url = _normalize_url(row["url"])
                if url:
                    return url

        cur.execute(
            "SELECT url FROM event_images WHERE event_id=? LIMIT 1",
            (event_id,),
        )
        row = cur.fetchone()
        if row and row["url"]:
            url = _normalize_url(row["url"])
            if url:
                return url

    # 2) raw_attributes_json (Apple / ESPN blobs)
    raw_json = event.get("raw_attributes_json")
    if not raw_json:
        return None

    try:
        attrs = json.loads(raw_json)
    except Exception:
        return None

    # 2a) Root Apple images (this is where your desired schema lives)
    images = attrs.get("images") or {}
    if isinstance(images, dict):
        # IMPORTANT: prefer shelfItemImagePost first
        root_pref = [
            "shelfItemImagePost",   # -> gen/1280x720Sports.TVAPoM02.jpg?... (your example)
            "shelfItemImage",
            "shelfItemImageLive",
            "shelfImageLogo",
            # Generic/ESPN-ish fallbacks
            "hero",
            "scene169",
            "landscape",
            "posterArt",
        ]
        for key in root_pref:
            if key in images:
                url = _extract(images[key])
                if url:
                    return url

        # As a last resort, any root image
        for key, val in images.items():
            url = _extract(val)
            if url:
                return url

    # 3) Competitor team logos – skip masterArtLogo
    competitors = attrs.get("competitors") or []
    if isinstance(competitors, list):
        preferred_keys = ["teamLogoDark", "teamLogoLight", "teamLogo", "scoreLogo", "eventLogo"]

        # Preferred keys
        for comp in competitors:
            if not isinstance(comp, dict):
                continue
            imgs = comp.get("images") or {}
            if not isinstance(imgs, dict):
                continue
            for key in preferred_keys:
                if key in imgs:
                    url = _extract(imgs[key])
                    if url:
                        return url

        # Any other competitor image EXCEPT masterArtLogo
        for comp in competitors:
            if not isinstance(comp, dict):
                continue
            imgs = comp.get("images") or {}
            if not isinstance(imgs, dict):
                continue
            for key, val in imgs.items():
                if key == "masterArtLogo":
                    continue
                url = _extract(val)
                if url:
                    return url

        # Fallback competitor.logo_url
        for comp in competitors:
            if not isinstance(comp, dict):
                continue
            url = _normalize_url(comp.get("logo_url"))
            if url:
                return url

    # 4) Playable-level images
    playables = attrs.get("playables") or []
    if isinstance(playables, list):
        for playable in playables:
            if not isinstance(playable, dict):
                continue
            for key in ("image", "cardImage", "imageUrl", "image_url"):
                if key in playable:
                    url = _extract(playable[key])
                    if url:
                        return url

    return None

# -------------------- Lanes XMLTV --------------------
def build_lanes_xmltv(conn: sqlite3.Connection, xml_path: str, epg_prefix: str = "lane."):
    """Export lanes schedule to XMLTV"""
    
    # Get all lanes
    cur = conn.cursor()
    cur.execute("SELECT * FROM lanes ORDER BY lane_id")
    lanes = [dict(row) for row in cur.fetchall()]
    
    if not lanes:
        print("No lanes found in database!")
        return
    
    print(f"Lanes XMLTV: {len(lanes)} virtual channels")
    
    # Get all lane events (include raw_attributes_json so we can use Apple images)
    cur.execute("""
        SELECT le.*,
               e.title,
               e.synopsis,
               e.channel_name,
               e.genres_json,
               e.pvid,
               e.raw_attributes_json
          FROM lane_events le
          LEFT JOIN events e ON le.event_id = e.id
         ORDER BY le.lane_id, datetime(le.start_utc)
    """)
    lane_events: Dict[int, List[Dict]] = {}
    for row in cur.fetchall():
        lane_id = row["lane_id"]
        if lane_id not in lane_events:
            lane_events[lane_id] = []
        lane_events[lane_id].append(dict(row))
    
    # Build XMLTV
    tv = ET.Element("tv")
    tv.set("generator-info-name", "FruitDeepLinks - Lanes")
    tv.set("generator-info-url", "https://github.com/yourusername/FruitDeepLinks")
    
    # Create channels
    for lane in lanes:
        lane_id = lane["lane_id"]
        chan_id = f"{epg_prefix}{lane_id}"
        
        chan = ET.SubElement(tv, "channel", id=chan_id)
        dn = ET.SubElement(chan, "display-name")
        dn.text = lane.get("name") or f"Sports Lane {lane_id}"
        
        # Add channel number if it exists
        if lane.get("logical_number"):
            dn2 = ET.SubElement(chan, "display-name")
            dn2.text = str(lane["logical_number"])
    
    # Add programmes
    total_programmes = 0
    for lane in lanes:
        lane_id = lane["lane_id"]
        chan_id = f"{epg_prefix}{lane_id}"
        events = lane_events.get(lane_id, [])
        
        for event in events:
            start_utc = parse_iso(event["start_utc"])
            end_utc = parse_iso(event["end_utc"])
            
            if end_utc <= start_utc:
                end_utc = start_utc + timedelta(hours=1)
            
            prog = ET.SubElement(
                tv,
                "programme",
                channel=chan_id,
                start=xmltv_time(start_utc),
                stop=xmltv_time(end_utc),
            )
            
            # Title
            title = event.get("title") or "Sports Event"
            ET.SubElement(prog, "title").text = title
            
            # Description
            desc = event.get("synopsis") or title
            channel_name = event.get("channel_name")
            if channel_name:
                provider = get_provider_from_channel(channel_name)
                desc = f"{desc} - on {provider}"
            ET.SubElement(prog, "desc").text = desc
            
            # Categories
            if channel_name:
                provider = get_provider_from_channel(channel_name)
                ET.SubElement(prog, "category").text = provider
            ET.SubElement(prog, "category").text = "Sports"
            
            # Genres
            genres_json = event.get("genres_json")
            if genres_json:
                try:
                    genres = json.loads(genres_json)
                    if isinstance(genres, list):
                        for g in genres:
                            if g and g != "Sports":
                                ET.SubElement(prog, "category").text = str(g)
                except Exception:
                    pass
            
            # Image (uses same Apple shelf logic as direct exporter)
            img_url = get_event_image_url(conn, event)
            if img_url:
                ET.SubElement(prog, "icon", src=img_url)
            
            total_programmes += 1
    
    print(f"Lanes XMLTV: {total_programmes} programmes scheduled")
    
    # Write file
    xml_str = minidom.parseString(ET.tostring(tv)).toprettyxml(indent="  ")
    Path(xml_path).parent.mkdir(parents=True, exist_ok=True)
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(xml_str)
    print(f"Wrote Lanes XMLTV: {xml_path}")

# -------------------- Lanes M3U --------------------
def build_lanes_m3u(conn: sqlite3.Connection, m3u_path: str, server_url: str, epg_prefix: str = "lane."):
    """Export lanes to M3U playlist"""
    
    # Get all lanes
    cur = conn.cursor()
    cur.execute("SELECT * FROM lanes ORDER BY lane_id")
    lanes = [dict(row) for row in cur.fetchall()]
    
    if not lanes:
        print("No lanes found in database!")
        return
    
    print(f"Lanes M3U: {len(lanes)} virtual channels")
    
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        
        for lane in lanes:
            lane_id = lane["lane_id"]
            chan_id = f"{epg_prefix}{lane_id}"
            name = lane.get("name") or f"Sports Lane {lane_id}"
            chno = lane.get("logical_number") or lane_id
            
            # Stream URL points to your server's lane endpoint
            stream_url = f"{server_url}/lanes/{lane_id}/stream.m3u8"
            
            f.write(f'#EXTINF:-1 tvg-id="{chan_id}" tvg-chno="{chno}" group-title="Sports Lanes",{name}\n')
            f.write(f"{stream_url}\n\n")
    
    Path(m3u_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"Wrote Lanes M3U: {m3u_path}")

# -------------------- Chrome Capture M3U --------------------
def build_chrome_m3u(conn: sqlite3.Connection, m3u_path: str, server_url: str, epg_prefix: str = "lane."):
    """Export lanes to Chrome Capture M3U playlist"""
    
    # Get all lanes
    cur = conn.cursor()
    cur.execute("SELECT * FROM lanes ORDER BY lane_id")
    lanes = [dict(row) for row in cur.fetchall()]
    
    if not lanes:
        print("No lanes found in database!")
        return
    
    print(f"Chrome M3U: {len(lanes)} virtual channels")
    
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n\n")
        
        for lane in lanes:
            lane_id = lane["lane_id"]
            chan_id = f"{epg_prefix}{lane_id}"
            name = lane.get("name") or f"Sports Lane {lane_id}"
            chno = lane.get("logical_number") or lane_id
            
            # Chrome Capture URL format
            stream_url = f"chrome://{server_url}/api/lane/{lane_id}/deeplink?format=text"
            
            f.write(f'#EXTINF:-1 tvg-id="{chan_id}" tvg-name="{name}" tvg-chno="{chno}" group-title="Sports Lanes",{name}\n')
            f.write(f"{stream_url}\n\n")
    
    Path(m3u_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"Wrote Chrome M3U: {m3u_path}")

# -------------------- CLI --------------------
def main():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent if script_dir.name == 'bin' else script_dir
    
    default_db = str(repo_root / 'data' / 'fruit_events.db')
    default_xml = str(repo_root / 'out' / 'multisource_lanes.xml')
    default_m3u = str(repo_root / 'out' / 'multisource_lanes.m3u')
    default_chrome_m3u = str(repo_root / 'out' / 'multisource_lanes_chrome.m3u')
    
    ap = argparse.ArgumentParser(description="Export virtual channel lanes to XMLTV/M3U")
    ap.add_argument("--db", default=(os.getenv("FRUIT_DB_PATH") or os.getenv("PEACOCK_DB_PATH") or default_db))
    ap.add_argument("--xml", default=default_xml, help="Output XMLTV file")
    ap.add_argument("--m3u", default=default_m3u, help="Output M3U playlist")
    ap.add_argument("--chrome-m3u", default=default_chrome_m3u, help="Output Chrome Capture M3U")
    ap.add_argument("--server-url", default=os.getenv("SERVER_URL", "http://192.168.86.72:6655"), help="Base URL for lane streams")
    ap.add_argument("--epg-prefix", default="lane.", help="Prefix for channel IDs")
    args = ap.parse_args()
    
    print(f"Using DB: {args.db}")
    print(f"Lanes outputs: {args.xml}, {args.m3u}, {args.chrome_m3u}")
    print(f"Server URL: {args.server_url}\n")
    
    conn = get_conn(args.db)
    
    # Check required tables
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row["name"] for row in cur.fetchall()}
    
    if "lanes" not in tables or "lane_events" not in tables:
        print("ERROR: Missing lanes or lane_events table!")
        print("Run fruit_build_lanes.py first to create the schedule.")
        return 1
    
    build_lanes_xmltv(conn, args.xml, epg_prefix=args.epg_prefix)
    build_lanes_m3u(conn, args.m3u, args.server_url, epg_prefix=args.epg_prefix)
    build_chrome_m3u(conn, args.chrome_m3u, args.server_url, epg_prefix=args.epg_prefix)
    
    conn.close()
    print("\nLanes export complete!")
    return 0

if __name__ == "__main__":
    sys.exit(main())

