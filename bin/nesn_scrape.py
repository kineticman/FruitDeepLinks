#!/usr/bin/env python3
"""
nesn_scrape.py - Scraper for NESN and NESN+ XMLTV schedules

Fetches public XMLTV feeds from NESN, parses programme data,
extracts show codes, and generates event IDs using EPlusTV formula.

Usage:
    python /app/bin/nesn_scrape.py --out /app/out/nesn_raw.json --days 7
    
Environment:
    NESN_DAYS: Number of days to scrape (default: 7)
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET
from urllib.request import Request, urlopen
from urllib.error import URLError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# NESN channel configuration
NESN_CHANNELS = {
    "NESN": {
        "feed_url": "https://epg.nesn.video/schedule-nesn.xml",
        "uuid": "ad866e7b-e58e-4f94-8eea-d6bcd4b5bfae",
        "xmltv_id": "85151538",
        "display_name": "New England Sports Network"
    },
    "NESN+": {
        "feed_url": "https://epg.nesn.video/schedule-nesnplus.xml",
        "uuid": "9c0955da-4dd7-45ce-bce3-e34997cb38e0",
        "xmltv_id": "535740538",
        "display_name": "New England Sports Network Plus"
    }
}


def extract_date_from_programme_id(programme_id: str) -> Optional[Tuple[int, str]]:
    """
    Extract date from NESN programme ID format.
    
    Formats:
    - YYMMDD: "BEC260208" -> Feb 8, 2026
    - YYMM: "BEC2602" -> Feb 2026 (use 1st of month)
    
    Returns:
        Tuple of (unix_timestamp_ms, date_str) or None if parsing fails
    """
    # Remove leading letters to find the date portion
    match = re.search(r'(\d{4}|\d{6})$', programme_id)
    if not match:
        return None
    
    date_part = match.group(1)
    
    try:
        if len(date_part) == 6:  # YYMMDD format
            year_yy = int(date_part[0:2])
            month = int(date_part[2:4])
            day = int(date_part[4:6])
        elif len(date_part) == 4:  # YYMM format
            year_yy = int(date_part[0:2])
            month = int(date_part[2:4])
            day = 1
        else:
            return None
        
        # Convert YY to YYYY (assume 20xx for 00-99)
        year = 2000 + year_yy if year_yy <= 99 else year_yy
        
        # Create date object
        dt = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
        unix_timestamp_ms = int(dt.timestamp() * 1000)
        date_str = dt.strftime("%Y-%m-%d")
        
        return (unix_timestamp_ms, date_str)
    except (ValueError, OverflowError) as e:
        logger.warning(f"Failed to parse date from programme_id '{programme_id}': {e}")
        return None


def extract_show_code(programme_id: str) -> Optional[str]:
    """
    Extract show code from NESN programme ID.
    
    Examples:
    - "BEC260208" -> "BEC"
    - "HUB2602" -> "HUB"
    - "FAIR260315" -> "FAIR"
    
    Returns:
        Show code (3-4 letter prefix) or None if not found
    """
    # Match 3-4 letter prefix at start
    match = re.match(r'^([A-Z]{3,4})', programme_id)
    if match:
        return match.group(1)
    return None


def parse_xmltv_datetime(dt_str: str) -> Optional[Tuple[int, str]]:
    """
    Parse XMLTV datetime format to UTC timestamp and ISO8601 string.
    
    XMLTV format: "20260208190000 +0000"
    
    Returns:
        Tuple of (unix_timestamp_ms, iso8601_str) or None
    """
    if not dt_str:
        return None
    
    try:
        # Extract just the datetime part (ignore timezone)
        dt_part = dt_str.split()[0]
        
        # Parse: YYYYMMDDHHMMSS
        dt = datetime.strptime(dt_part, "%Y%m%d%H%M%S")
        dt = dt.replace(tzinfo=timezone.utc)
        
        unix_timestamp_ms = int(dt.timestamp() * 1000)
        iso8601 = dt.isoformat().replace("+00:00", "Z")
        
        return (unix_timestamp_ms, iso8601)
    except (ValueError, AttributeError) as e:
        logger.warning(f"Failed to parse datetime '{dt_str}': {e}")
        return None


def calculate_end_time(start_ms: int, duration_secs: int) -> Tuple[int, str]:
    """
    Calculate end time from start time and duration.
    
    Args:
        start_ms: Start time in milliseconds
        duration_secs: Duration in seconds
    
    Returns:
        Tuple of (end_ms, end_utc_str)
    """
    end_ms = start_ms + (duration_secs * 1000)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    end_utc = end_dt.isoformat().replace("+00:00", "Z")
    return (end_ms, end_utc)


def fetch_xmltv_feed(url: str) -> Optional[ET.Element]:
    """
    Fetch and parse XMLTV feed from URL using urllib (simple, robust method).
    
    Returns:
        ElementTree root element or None if fetch fails
    """
    try:
        logger.info(f"Fetching {url}...")
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            data = response.read()
        logger.info(f"✓ Fetched {len(data)} bytes")
        
        # Parse XML
        root = ET.fromstring(data)
        return root
    except (URLError, ET.ParseError) as e:
        logger.error(f"✗ Failed to fetch/parse {url}: {e}")
        return None


def parse_xmltv(root: ET.Element, channel_name: str, channel_config: Dict) -> List[Dict]:
    """
    Parse XMLTV root element and extract programmes.
    
    Args:
        root: ElementTree root element from ET.fromstring()
        channel_name: Name of channel (NESN or NESN+)
        channel_config: Channel configuration dict
    
    Returns:
        List of normalized programme events
    """
    events = []
    
    try:
        # Find all programmes in the XML tree
        programmes = root.findall('.//programme')
        logger.info(f"Found {len(programmes)} programmes in {channel_name}")
        
        for prog in programmes:
            try:
                # Extract programme ID
                programme_id = prog.get('id', '')
                if not programme_id:
                    continue
                
                # Extract show code and date
                show_code = extract_show_code(programme_id)
                date_result = extract_date_from_programme_id(programme_id)
                
                if not show_code or not date_result:
                    logger.debug(f"Skipping programme '{programme_id}': no code or date")
                    continue
                
                date_timestamp_ms, date_str = date_result
                
                # Extract title and description
                title_elem = prog.find('title')
                title = title_elem.text if title_elem is not None else ""
                
                desc_elem = prog.find('desc')
                description = desc_elem.text if desc_elem is not None else ""
                
                # Extract sub-type from XMLTV
                # Values: "(R)" = Replay/Repeat, "NEW" = New episode, "LIVE" = Live broadcast
                sub_type_elem = prog.find('sub-type')
                sub_type = sub_type_elem.text.strip() if sub_type_elem is not None and sub_type_elem.text else None
                
                # Map XMLTV sub-type to replay flag and airing_type
                is_replay = False
                airing_type = "premiere"  # Default
                
                if sub_type:
                    if sub_type == "(R)":
                        # (R) = Replay/Repeat - established content
                        is_replay = True
                        airing_type = "replay"
                    elif sub_type == "NEW":
                        # NEW = First airing/new episode
                        is_replay = False
                        airing_type = "premiere"
                    elif sub_type == "LIVE":
                        # LIVE = Live broadcast
                        is_replay = False
                        airing_type = "live"
                
                # Duration extraction
                duration_elem = prog.find('length')
                duration_secs = 0
                if duration_elem is not None and duration_elem.text:
                    try:
                        duration_secs = int(duration_elem.text)
                    except ValueError:
                        logger.warning(f"Invalid duration for {programme_id}: {duration_elem.text}")
                        duration_secs = 5400  # Default to 1.5 hours
                
                # Extract image
                icon_elem = prog.find('icon')
                image_url = icon_elem.get('src', '') if icon_elem is not None else ""
                
                # Extract start/end times from XMLTV
                start_xmltv = prog.get('start', '')
                end_xmltv = prog.get('stop', '')
                
                start_result = parse_xmltv_datetime(start_xmltv)
                end_result = parse_xmltv_datetime(end_xmltv)
                
                # Use XMLTV times if available, otherwise calculate from date + duration
                if start_result:
                    start_ms, start_utc = start_result
                else:
                    start_ms = date_timestamp_ms
                    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
                    start_utc = start_dt.isoformat().replace("+00:00", "Z")
                
                if end_result:
                    end_ms, end_utc = end_result
                else:
                    end_ms, end_utc = calculate_end_time(start_ms, duration_secs)
                
                # Recalculate duration if we have both times
                if start_ms and end_ms:
                    duration_secs = int((end_ms - start_ms) / 1000)
                
                # Extract categories from XMLTV (if available) and detect sports
                categories = [channel_name]  # Always include channel name (NESN or NESN+)
                
                # Try to extract category elements from XMLTV
                # Skip channel-related categories like "NESN+", "NESN", etc - we already have channel_name
                for cat_elem in prog.findall('category'):
                    if cat_elem.text:
                        # Skip channel names and generic categories
                        cat_text = cat_elem.text.strip()
                        if cat_text.upper() not in ('NESN', 'NESN+', 'SPORTS', 'SPORTS NETWORK'):
                            if cat_text not in categories:
                                categories.append(cat_text)
                
                # Detect sports from title and description for better categorization
                combined_text = (title + " " + description).lower()
                
                sports_detected = set()
                if 'hockey' in combined_text or 'bruins' in combined_text or 'beanpot' in combined_text:
                    sports_detected.add('Hockey')
                if 'baseball' in combined_text or 'red sox' in combined_text or 'sox' in combined_text or 'mlb' in combined_text:
                    sports_detected.add('Baseball')
                if 'basketball' in combined_text or 'celtics' in combined_text or 'nba' in combined_text:
                    sports_detected.add('Basketball')
                if 'football' in combined_text or 'patriots' in combined_text or 'nfl' in combined_text:
                    sports_detected.add('Football')
                if 'soccer' in combined_text or 'liverpool' in combined_text or 'manchester' in combined_text:
                    sports_detected.add('Soccer')
                
                # Add detected sports to categories (avoid duplicates)
                for sport in sorted(sports_detected):
                    if sport not in categories:
                        categories.append(sport)
                
                # Generate event ID using EPlusTV formula
                external_id = f"tvschedule-{start_ms}-{show_code}"
                
                event = {
                    "external_id": external_id,
                    "programme_id": programme_id,
                    "show_code": show_code,
                    "title": title,
                    "channel": channel_name,
                    "channel_uuid": channel_config["uuid"],
                    "xmltv_id": channel_config["xmltv_id"],
                    "start_utc": start_utc,
                    "end_utc": end_utc,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "duration_secs": duration_secs,
                    "description": description,
                    "image_url": image_url,
                    "sport": "",  # NESN has mixed content
                    "categories": categories,
                    "sub_type": sub_type,  # XMLTV value: "(R)", "NEW", "LIVE"
                    "airing_type": airing_type,  # Mapped value: "replay", "premiere", "live"
                    "replay": is_replay  # Boolean: True if (R), False otherwise
                }
                
                events.append(event)
                
            except Exception as e:
                logger.warning(f"Error parsing programme {programme_id}: {e}")
                continue
        
        logger.info(f"✓ Parsed {len(events)} valid events from {channel_name}")
        return events
        
    except ET.ParseError as e:
        logger.error(f"✗ Failed to parse XML for {channel_name}: {e}")
        return []


def scrape_nesn(days: int = 7) -> Dict:
    """
    Scrape NESN and NESN+ XMLTV feeds.
    
    Args:
        days: Number of days to include (for future validation)
    
    Returns:
        Dictionary with all events from both channels
    """
    logger.info(f"=== NESN Scraper Starting (days={days}) ===")
    
    all_events = []
    show_codes_found = set()
    
    for channel_name, channel_config in NESN_CHANNELS.items():
        logger.info(f"\n>>> Processing {channel_name}")
        
        # Fetch and parse XMLTV feed
        root = fetch_xmltv_feed(channel_config["feed_url"])
        if root is None:
            logger.warning(f"Skipping {channel_name} due to fetch failure")
            continue
        
        # Parse XMLTV
        events = parse_xmltv(root, channel_name, channel_config)
        
        # Track show codes
        for event in events:
            show_codes_found.add(event["show_code"])
        
        all_events.extend(events)
    
    logger.info(f"\n=== Scrape Summary ===")
    logger.info(f"Total events: {len(all_events)}")
    logger.info(f"Unique show codes: {len(show_codes_found)}")
    logger.info(f"Show codes: {', '.join(sorted(show_codes_found))}")
    
    return {
        "scraped_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_events": len(all_events),
        "unique_show_codes": len(show_codes_found),
        "show_codes": sorted(show_codes_found),
        "events": all_events
    }


def main():
    parser = argparse.ArgumentParser(
        description="Scrape NESN and NESN+ XMLTV feeds"
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Output JSON file path"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to scrape (default: 7)"
    )
    
    args = parser.parse_args()
    
    # Allow environment variable override
    days = int(os.environ.get("NESN_DAYS", args.days))
    
    # Scrape
    result = scrape_nesn(days=days)
    
    # Write output
    try:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Wrote {len(result['events'])} events to {args.out}")
    except Exception as e:
        logger.error(f"✗ Failed to write output: {e}")
        sys.exit(1)
    
    logger.info(f"\n=== Scrape Complete ===")


if __name__ == "__main__":
    main()
